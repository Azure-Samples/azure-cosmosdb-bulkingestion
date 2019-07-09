/**
 * The MIT License (MIT) Copyright (c) 2017 Microsoft Corporation
 *
 * <p>Permission is hereby granted, free of charge, to any person obtaining a copy of this software
 * and associated documentation files (the "Software"), to deal in the Software without restriction,
 * including without limitation the rights to use, copy, modify, merge, publish, distribute,
 * sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * <p>The above copyright notice and this permission notice shall be included in all copies or
 * substantial portions of the Software.
 *
 * <p>THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING
 * BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
 * NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM,
 * DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */

package com.microsoft.azure.cosmosdb.sql.jsonstoreimport;

import com.google.common.collect.Lists;
import com.google.gson.Gson;
import com.microsoft.azure.cosmosdb.sql.jsonstoreimport.runnables.PartitionIngestionRunnable;
import com.microsoft.azure.cosmosdb.sql.jsonstoreimport.runnables.ReadDocumentGroupRunnable;
import com.microsoft.azure.cosmosdb.sql.jsonstoreimport.sdkextensions.CosmosDbSqlClientExtension;
import com.microsoft.azure.cosmosdb.sql.jsonstoreimport.sink.CosmosDbSqlWriter;
import com.microsoft.azure.cosmosdb.sql.jsonstoreimport.source.JsonStoreEntity;
import com.microsoft.azure.cosmosdb.sql.jsonstoreimport.source.JsonStoreEntityImportResponse;
import com.microsoft.azure.cosmosdb.sql.jsonstoreimport.source.JsonStoreReader;
import com.microsoft.azure.cosmosdb.sql.jsonstoreimport.source.ScaleTestReader;
import com.microsoft.azure.documentdb.ConnectionPolicy;
import com.microsoft.azure.documentdb.ConsistencyLevel;
import com.microsoft.azure.documentdb.Document;
import com.microsoft.azure.documentdb.DocumentClientException;
import com.microsoft.azure.documentdb.DocumentCollection;
import com.microsoft.azure.documentdb.PartitionKeyDefinition;
import com.microsoft.azure.documentdb.RetryOptions;
import com.microsoft.azure.documentdb.internal.routing.PartitionKeyInternal;
import com.microsoft.azure.documentdb.internal.routing.PartitionKeyInternalHelper;
import com.microsoft.azure.documentdb.internal.routing.Range;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Stack;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.log4j.Logger;

class ImportExecutor {
  private final Logger logger = Logger.getLogger(ImportExecutor.class);
  private CosmosDbSqlClientExtension importTrackingClient;
  private CosmosDbSqlClientExtension ingestionClient;
  private com.microsoft.azure.documentdb.bulkexecutor.DocumentBulkExecutor bulkImporter;
  private PartitionKeyDefinition cosmosDbPartitionKeyDefinition = null;
  private int cosmosDbImportMaxMiniBatchSizeInBytes =
      Settings.getCosmosDbImportMiniBatchMaxSizeInBytes();
  private Gson gson = new Gson();

  ImportExecutor(boolean isTrackingRequired) throws DocumentClientException {

    if (isTrackingRequired) {
      initImportTrackingClient();
    }
  }

  static void startPrecookingDataThread() {
    Runnable r =
        () -> {
          try {
            ScaleTestReader.preCookData();
            Thread.sleep(300);
          } catch (IOException | InterruptedException e) {
            e.printStackTrace();
          } catch (Exception e) {
            e.printStackTrace();
          }
        };
    new Thread(r).start();
  }

  void queueItems(StoreType storeType, String location, String operation) throws Exception {
    new ImportQueue(importTrackingClient).pushEntities(storeType, location, operation);
  }

  void queueItemsByPartition(StoreType storeType, String location, String operation)
      throws Exception {
    new ImportQueue(importTrackingClient).pushEntitiesByPartition(storeType, location, operation);
  }

  void processQueue() throws Exception {

    // Read the first set of documents and add to stack
    Stack<DocumentGroup> documentGroupStack = new Stack<>();
    ImportWorkItem firstJsonStoreEntity = new ImportQueue(this.importTrackingClient).popEntity();
    if (firstJsonStoreEntity == null) {
      logger.info("Cosmos DB Queue is empty, please add items.");
      return;
    }
    final JsonStoreEntity firstJsonStoreEntityJsonStoreEntityInstance =
        firstJsonStoreEntity.getJsonStoreEntityInstance();
    JsonStoreReader jsonStoreReader =
        new JsonStoreReader(firstJsonStoreEntityJsonStoreEntityInstance);
    List<String> firstDocs = jsonStoreReader.getJsonDocs();
    documentGroupStack.add(new DocumentGroup(firstJsonStoreEntity, firstDocs));

    while (true) {
      // Start reading the next available ADL file and add to stack
      ImportWorkItem nextJsonStoreEntity = new ImportQueue(this.importTrackingClient).popEntity();
      ReadDocumentGroupRunnable stackRunnable =
          new ReadDocumentGroupRunnable(documentGroupStack, nextJsonStoreEntity);
      Thread readNextDocumentGroup = new Thread(stackRunnable);
      readNextDocumentGroup.start();

      // Insert the next available batch into Cosmos DB
      DocumentGroup currentDocGroup = documentGroupStack.pop();
      if (currentDocGroup != null) {
        initBulkImportDocumentClient(false);
        logger.info(
            "Writing documents to CosmosDb for "
                + currentDocGroup.importWorkItem.getMappedCosmosDbDocument().getId());
        int cosmosDbBulkImportLibBatchSize = Settings.getCosmosDbBulkImportLibBatchSize();
        if (cosmosDbBulkImportLibBatchSize < 0) {
          cosmosDbBulkImportLibBatchSize = currentDocGroup.jsonDocs.size();
        }
        JsonStoreEntityImportResponse jsonStoreEntityImportResponse =
            processJsonDocsInBatches(currentDocGroup.jsonDocs, cosmosDbBulkImportLibBatchSize);

        JsonStoreEntity currentJsonStoreEntityInstance =
            currentDocGroup.importWorkItem.getJsonStoreEntityInstance();
        currentJsonStoreEntityInstance.jsonStoreEntityImportResponse =
            jsonStoreEntityImportResponse;
        currentJsonStoreEntityInstance.isComplete = true;
        currentJsonStoreEntityInstance.isInProgress = false;
        currentJsonStoreEntityInstance.completeDate = Instant.now();
        this.importTrackingClient.updateItem(
            currentDocGroup.importWorkItem.getMappedCosmosDbDocument(),
            gson.toJson(currentJsonStoreEntityInstance));
      }
      if (documentGroupStack.isEmpty()) {
        if (nextJsonStoreEntity == null) {
          // This means that all the files to be written are finished
          break;
        }
        while (documentGroupStack.isEmpty()) {
          logger.info("Waiting for stack to fill up");
          TimeUnit.SECONDS.sleep(1);
        }
      }
    }
  }

  public void processSortedDataByPartition() throws Exception {
    // Claim partitions from tracking
    initBulkImportDocumentClient(false);
    int noOfPartitionsAllowedToClaim = Settings.getPartitionsLimitForWorker();
    ImportQueue importQueue = new ImportQueue(this.importTrackingClient);
    List<ImportWorkItem> claims = importQueue.getPartitionClaims(noOfPartitionsAllowedToClaim);
    List<Thread> threads = new ArrayList<>();
    for (ImportWorkItem c : claims) {
      PartitionIngestionRunnable partitionIngestionRunnable =
          new PartitionIngestionRunnable(bulkImporter, c, importTrackingClient);
      Thread partitionIngestionThread = new Thread(partitionIngestionRunnable);
      partitionIngestionThread.start();
      threads.add(partitionIngestionThread);
    }

    for (Thread t : threads) {
      if (t != null) {
        t.join();
      }
    }
  }

  private JsonStoreEntityImportResponse processJsonDocsInBatches(
      List<String> jsonDocs, int batchSize) throws Exception {

    List<List<String>> batches = Lists.partition(jsonDocs, batchSize);

    List<JsonStoreEntityImportResponse> jsonStoreEntityImportResponseList = new ArrayList<>();
    for (List<String> batch : batches) {
      JsonStoreEntityImportResponse jsonStoreEntityImportResponse =
          new CosmosDbSqlWriter().ingestJsonStoreEntity(batch, bulkImporter);
      if (jsonStoreEntityImportResponse.isError()) {

        // force re-initiating bulk importer this should cover split scenario for new batches
        try {
          initBulkImportDocumentClient(true);
          // TODO, stop processing the file even for single error
          break;
        } catch (Exception e) {
          logger.error(
              String.format("Re-initializing bulk importer failed due to " + e.getMessage()));
        }
      }
      jsonStoreEntityImportResponseList.add(jsonStoreEntityImportResponse);
    }
    return JsonStoreEntityImportResponse.aggregate(jsonStoreEntityImportResponseList);
  }

  void processJsonStoreEntity(JsonStoreEntity jsonStoreEntity) throws Exception {
    initBulkImportDocumentClient(false);
    JsonStoreReader jsonStoreReader = new JsonStoreReader(jsonStoreEntity);
    new CosmosDbSqlWriter().ingestJsonStoreEntity(jsonStoreReader.getJsonDocs(), bulkImporter);
  }

  private void initIngestionClient() throws DocumentClientException {
    if (this.ingestionClient != null) {
      return;
    }

    logger.info("Initializing Ingestion client");
    ConnectionPolicy connectionPolicy = new ConnectionPolicy();
    RetryOptions retryOptions = new RetryOptions();
    // set to 0 to let bulk importer handles throttling
    retryOptions.setMaxRetryAttemptsOnThrottledRequests(0);
    retryOptions.setMaxRetryWaitTimeInSeconds(0);
    connectionPolicy.setRetryOptions(retryOptions);
    connectionPolicy.setMaxPoolSize(Settings.getCosmosDbDataCollectionConnectionPoolSize());
    this.ingestionClient =
        new CosmosDbSqlClientExtension(
            Settings.getCosmosDbEndPoint().trim(),
            Settings.getCosmosDbMasterKey().trim(),
            Settings.getCosmosDbDatabase().trim(),
            Settings.getCosmosDbDataCollection().trim(),
            Settings.getCosmosDbDataCollectionThroughput(),
            ConsistencyLevel.Eventual,
            connectionPolicy);
    this.ingestionClient.createDatabaseIfNotExists();
    DocumentCollection retrievedCollection =
        this.ingestionClient.createCollectionIfNotExists(
            "/" + Settings.getCosmosDbDataCollectionPkValue());
    cosmosDbPartitionKeyDefinition = retrievedCollection.getPartitionKey();
  }

  private void initImportTrackingClient() throws DocumentClientException {
    if (this.importTrackingClient != null) {
      return;
    }
    this.importTrackingClient =
        new CosmosDbSqlClientExtension(
            Settings.getCosmosDbEndPoint().trim(),
            Settings.getCosmosDbMasterKey().trim(),
            Settings.getCosmosDbDatabase().trim(),
            Settings.getCosmosDbImportTrackingCollection().trim(),
            Settings.getImportTrackingCollectionThroughput(),
            null,
            null);
    this.importTrackingClient.createDatabaseIfNotExists();
    this.importTrackingClient.createCollectionIfNotExists("/id");
  }

  private void initBulkImportDocumentClient(boolean forceInitiate) throws Exception {
    if (bulkImporter == null) {
      logger.info("Initiating bulk importer");
      initIngestionClient();
      DocumentCollection cosmosDbCol = ingestionClient.getCollection();
      cosmosDbPartitionKeyDefinition = cosmosDbCol.getPartitionKey();
      int cosmosDbThroughPut = ingestionClient.getOfferThroughput();
      // this assumes database and collection already exists
      com.microsoft.azure.documentdb.bulkexecutor.DocumentBulkExecutor.Builder bulkImporterBuilder =
          com.microsoft.azure.documentdb.bulkexecutor.DocumentBulkExecutor.builder()
              .from(
                  ingestionClient.getDocumentClient(),
                  ingestionClient.getDatabaseName(),
                  ingestionClient.getCollectionName(),
                  cosmosDbPartitionKeyDefinition,
                  cosmosDbThroughPut);
      if (cosmosDbImportMaxMiniBatchSizeInBytes > 0) {
        bulkImporterBuilder.withMaxMiniBatchSize(cosmosDbImportMaxMiniBatchSizeInBytes);
      }
      bulkImporter = bulkImporterBuilder.build();
    }
    if (forceInitiate) {
      logger.info("closing bulk importer and re-initiating");
      safeClose(bulkImporter);
      ingestionClient.safeClose();
      ingestionClient = null;
      // set to null and re-initiate
      bulkImporter = null;
      initBulkImportDocumentClient(false);
    }
  }

  void initScaleTest() throws Exception {
    startPrecookingDataThread();
    JsonStoreEntity jsonStoreEntity = new JsonStoreEntity();
    jsonStoreEntity.storeType = StoreType.TEST;
    jsonStoreEntity.owner = ManagementFactory.getRuntimeMXBean().getName();
    jsonStoreEntity.id = UUID.randomUUID().toString();
    jsonStoreEntity.name = Settings.getRunTag();
    int run = 1;
    while (true) {
      jsonStoreEntity.location = "Run " + run;
      jsonStoreEntity.id = UUID.randomUUID().toString();
      logger.info("Initiating CosmosDb import...");
      initBulkImportDocumentClient(false);
      JsonStoreReader jsonStoreReader = new JsonStoreReader(jsonStoreEntity);
      JsonStoreEntityImportResponse jsonStoreEntityImportResponse =
          new CosmosDbSqlWriter()
              .ingestJsonStoreEntity(jsonStoreReader.getJsonDocs(), bulkImporter);
      if (jsonStoreEntityImportResponse.isError()) {

        // force re-initiating bulk importer this should cover split scenario for new batches
        try {
          initBulkImportDocumentClient(true);
        } catch (Exception e) {
          logger.error(
              String.format(
                  "Re-initialing bulk importer failed: due to %s. " + "Will retry Later",
                  e.getMessage()));
        }
      }
      jsonStoreEntity.jsonStoreEntityImportResponse = jsonStoreEntityImportResponse;
      jsonStoreEntity.isComplete = true;
      jsonStoreEntity.isInProgress = false;
      jsonStoreEntity.completeDate = Instant.now();
      this.importTrackingClient.createDocument(new Document(gson.toJson(jsonStoreEntity)));
      run++;
    }
  }

  private void safeClose(AutoCloseable util) {
    if (util != null) {
      try {
        util.close();
      } catch (Exception e) {
        logger.info("closing client failed with %s. Will re-initialize later" + e.getMessage());
      }
    }
  }
}
