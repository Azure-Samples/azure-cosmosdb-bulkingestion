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

import com.google.gson.Gson;
import com.microsoft.azure.cosmosdb.sql.jsonstoreimport.sdkextensions.AdlStoreClientExtension;
import com.microsoft.azure.cosmosdb.sql.jsonstoreimport.sdkextensions.AzureBlobClientExtension;
import com.microsoft.azure.cosmosdb.sql.jsonstoreimport.sdkextensions.CosmosDbSqlClientExtension;
import com.microsoft.azure.cosmosdb.sql.jsonstoreimport.source.JsonStoreEntity;
import com.microsoft.azure.documentdb.Document;
import com.microsoft.azure.documentdb.DocumentClientException;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.net.URLEncoder;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import org.apache.http.HttpStatus;
import org.apache.log4j.Logger;

public class ImportQueue {

  private static final Logger logger = Logger.getLogger(ImportQueue.class);
  private CosmosDbSqlClientExtension queueWriter;
  private Gson gson = new Gson();

  public ImportQueue(CosmosDbSqlClientExtension queueWriter) {
    this.queueWriter = queueWriter;
  }

  public List<ImportWorkItem> getPartitionImportWorkItems(String partitionId)
      throws DocumentClientException {
    List<ImportWorkItem> partitionFilesList = new ArrayList<>();
    // TODO: What to do about abandoned files where isInProgress = true?
    String query =
        "select * from c where c.isInProgress = false and c.isComplete = false and "
            + "c.operation='"
            + Constants.OPERATION_PARTITION_FILE_WRITE
            + "' and "
            + "c.partitionId='"
            + partitionId
            + "'";
    List<Document> docs = this.queueWriter.queryDocs(query);
    for (Document doc : docs) {
      JsonStoreEntity jsonStoreEntity = gson.fromJson(doc.toString(), JsonStoreEntity.class);
      logger.info("Found import file to process: " + jsonStoreEntity.name);

      String processId = ManagementFactory.getRuntimeMXBean().getName();

      jsonStoreEntity.owner = processId;
      logger.info("Claimed " + jsonStoreEntity.name);
      ImportWorkItem claim = new ImportWorkItem();
      claim.setMappedCosmosDbDocument(doc);
      claim.setJsonStoreEntityInstance(jsonStoreEntity);
      partitionFilesList.add(claim);
    }

    return partitionFilesList;
  }

  void pushEntities(StoreType storeType, String location, String operation) throws Exception {
    List<String> queueItems;
    switch (storeType) {
      case ADL:
        queueItems = AdlStoreClientExtension.getAdlFolderFileNames(location);
        break;
      case AZURE_BLOB:
        queueItems = AzureBlobClientExtension.getListOfBlobs(location);
        break;
      default:
        throw new Exception("Unsupported store type for queue.");
    }
    pushItemsToCosmosDb(queueItems, storeType, operation);
  }

  @SuppressWarnings("deprecation")
  private void pushItemsToCosmosDb(List<String> queueItems, StoreType storeType, String operation)
      throws DocumentClientException {

    logger.info(String.format("Inserting %d files into Cosmos DB work queue", queueItems.size()));
    for (int i = 0; i < queueItems.size(); i++) {
      JsonStoreEntity jsonStoreEntity = new JsonStoreEntity();
      jsonStoreEntity.id = URLEncoder.encode(queueItems.get(i));
      jsonStoreEntity.name = queueItems.get(i);
      jsonStoreEntity.location = queueItems.get(i);
      jsonStoreEntity.createDate = Instant.now();
      jsonStoreEntity.isComplete = false;
      jsonStoreEntity.isInProgress = false;
      jsonStoreEntity.storeType = storeType;
      jsonStoreEntity.operation = operation;

      logger.info("Inserting a file: " + jsonStoreEntity.name);
      queueWriter.createDocument(new Document(gson.toJson(jsonStoreEntity)));
    }
  }

  void pushEntitiesByPartition(StoreType storeType, String location, String operation)
      throws Exception {
    List<String> queueItemsByPartition;
    switch (storeType) {
      case ADL:
        queueItemsByPartition = AdlStoreClientExtension.getAdlFolderFileNames(location);
        break;
      case AZURE_BLOB:
        queueItemsByPartition = AzureBlobClientExtension.getListOfBlobs(location);
        break;
      default:
        throw new Exception("Unsupported store type for queue.");
    }
    pushItemsToCosmosDbByPartitionId(queueItemsByPartition, storeType, operation);
  }

  private void pushItemsToCosmosDbByPartitionId(
      List<String> queueItemsByPartition, StoreType storeType, String operation)
      throws DocumentClientException, IOException {

    logger.info(
        String.format(
            "Inserting %d files into Cosmos DB work queue", queueItemsByPartition.size()));
    for (int i = 0; i < queueItemsByPartition.size(); i++) {
      // Insert partition to claim by workers
      JsonStoreEntity partitionJsonStoreEntity = new JsonStoreEntity();
      partitionJsonStoreEntity.id = URLEncoder.encode(queueItemsByPartition.get(i));
      partitionJsonStoreEntity.name = queueItemsByPartition.get(i);
      partitionJsonStoreEntity.location = queueItemsByPartition.get(i);
      partitionJsonStoreEntity.createDate = Instant.now();
      partitionJsonStoreEntity.isComplete = false;
      partitionJsonStoreEntity.isInProgress = false;
      partitionJsonStoreEntity.storeType = storeType;
      partitionJsonStoreEntity.operation = operation;
      partitionJsonStoreEntity.partitionId = queueItemsByPartition.get(i);
      logger.info("Inserting a partition: " + partitionJsonStoreEntity.name);
      queueWriter.createDocument(new Document(gson.toJson(partitionJsonStoreEntity)));
      List<String> partitionItems =
          AdlStoreClientExtension.getAdlFolderFileNames(queueItemsByPartition.get(i));
      for (int j = 0; j < partitionItems.size(); j++) {
        JsonStoreEntity jsonStoreEntity = new JsonStoreEntity();
        jsonStoreEntity.id = URLEncoder.encode(partitionItems.get(j));
        jsonStoreEntity.name = partitionItems.get(j);
        jsonStoreEntity.location = partitionItems.get(j);
        jsonStoreEntity.createDate = Instant.now();
        jsonStoreEntity.isComplete = false;
        jsonStoreEntity.isInProgress = false;
        jsonStoreEntity.storeType = storeType;
        jsonStoreEntity.operation = Constants.OPERATION_PARTITION_FILE_WRITE;
        jsonStoreEntity.partitionId = queueItemsByPartition.get(i);
        logger.info("Inserting a file: " + jsonStoreEntity.name);
        queueWriter.createDocument(new Document(gson.toJson(jsonStoreEntity)));
      }
    }
  }

  List<ImportWorkItem> getPartitionClaims(int partitionsCount) throws DocumentClientException {
    int claimsCount = 0;
    List<ImportWorkItem> claimsList = new ArrayList<>();
    while (claimsCount < partitionsCount) {
      // TODO: What to do about abandoned files where isInProgress = true?
      List<Document> docs =
          this.queueWriter.queryDocs(
              "select top 10 * from c where c.isInProgress = false and c.isComplete = false and c"
                  + ".operation='"
                  + Constants.OPERATION_PARTITIONED_WRITES
                  + "'");
      if (docs.size() <= 0) {
        break;
      }

      // Pick first one and try to claim
      Document currentDocument = docs.get(0);

      JsonStoreEntity jsonStoreEntity =
          gson.fromJson(currentDocument.toString(), JsonStoreEntity.class);
      logger.info("Found import file to process: " + jsonStoreEntity.name);

      String processId = ManagementFactory.getRuntimeMXBean().getName();

      // Claim the file by locking it
      try {
        jsonStoreEntity.isInProgress = true;
        jsonStoreEntity.owner = processId;
        currentDocument =
            this.queueWriter.updateItem(currentDocument, gson.toJson(jsonStoreEntity));
        logger.info("Claimed " + jsonStoreEntity.name);
        ImportWorkItem claim = new ImportWorkItem();
        claim.setMappedCosmosDbDocument(currentDocument);
        claim.setJsonStoreEntityInstance(jsonStoreEntity);
        claimsList.add(claim);
        claimsCount++;
      } catch (DocumentClientException e) {
        if (e.getStatusCode() == HttpStatus.SC_PRECONDITION_FAILED) {
          logger.info("Cannot claim file because another worker acquired it");
        } else {
          if (e.getStatusCode() == 429) {
            logger.info("Cannot claim file because of throttling. Will retry");
          } else {
            throw e;
          }
        }
      }
    }
    return claimsList;
  }

  ImportWorkItem popEntity() throws DocumentClientException {
    while (true) {
      // TODO: What to do about abandoned files where isInProgress = true?
      List<Document> docs =
          this.queueWriter.queryDocs(
              "select top 10 * from c where c.isInProgress = false and c.isComplete = false");
      if (docs.size() <= 0) {
        break;
      }

      // Pick first one and try to claim
      Document currentDocument = docs.get(0);

      JsonStoreEntity jsonStoreEntity =
          gson.fromJson(currentDocument.toString(), JsonStoreEntity.class);
      logger.info("Found import file to process: " + jsonStoreEntity.name);

      String processId = ManagementFactory.getRuntimeMXBean().getName();

      // Claim the file by locking it
      try {
        jsonStoreEntity.isInProgress = true;
        jsonStoreEntity.owner = processId;
        currentDocument =
            this.queueWriter.updateItem(currentDocument, gson.toJson(jsonStoreEntity));
        logger.info("Claimed " + jsonStoreEntity.name);
        ImportWorkItem claim = new ImportWorkItem();
        claim.setMappedCosmosDbDocument(currentDocument);
        claim.setJsonStoreEntityInstance(jsonStoreEntity);
        return claim;

      } catch (DocumentClientException e) {
        if (e.getStatusCode() == HttpStatus.SC_PRECONDITION_FAILED) {
          logger.info("Cannot claim file because another worker acquired it");
        } else {
          if (e.getStatusCode() == 429) {
            logger.info("Cannot claim file because of throttling. Will retry");
          } else {
            throw e;
          }
        }
      }
    }
    return null;
  }
}
