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

package com.microsoft.azure.cosmosdb.sql.jsonstoreimport.runnables;

import com.google.gson.Gson;
import com.microsoft.azure.cosmosdb.sql.jsonstoreimport.ImportQueue;
import com.microsoft.azure.cosmosdb.sql.jsonstoreimport.ImportWorkItem;
import com.microsoft.azure.cosmosdb.sql.jsonstoreimport.Settings;
import com.microsoft.azure.cosmosdb.sql.jsonstoreimport.sdkextensions.CosmosDbSqlClientExtension;
import com.microsoft.azure.cosmosdb.sql.jsonstoreimport.source.JsonStoreEntity;
import com.microsoft.azure.cosmosdb.sql.jsonstoreimport.source.JsonStoreEntityImportResponse;
import com.microsoft.azure.documentdb.DocumentClientException;
import com.microsoft.azure.documentdb.bulkexecutor.DocumentBulkExecutor;
import com.microsoft.azure.documentdb.bulkexecutor.BulkImportResponse;
import java.io.IOException;
import java.text.MessageFormat;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.regex.Pattern;
import org.apache.log4j.Logger;

public class PartitionIngestionRunnable implements Runnable {
  private final Logger logger = Logger.getLogger(PartitionIngestionRunnable.class);
  private DocumentBulkExecutor bulkImporter;
  private ImportWorkItem importWorkItem;
  private CosmosDbSqlClientExtension importTrackingClient;
  private ImportWorkItem[] partitionImportWorkItems;
  private List<ImportWorkItem> sortedPartitionImportWorkItems;
  private int jsonDocColumnIndexInCsv;
  private String partitionId;
  private Gson gson = new Gson();
  private List<JsonStoreEntityImportResponse> jsonStoreEntityImportResponses;

  public PartitionIngestionRunnable(
      DocumentBulkExecutor bulkImporter,
      ImportWorkItem importWorkItem,
      CosmosDbSqlClientExtension importTrackingClient)
      throws DocumentClientException {
    this.bulkImporter = bulkImporter;
    this.importWorkItem = importWorkItem;
    this.importTrackingClient = importTrackingClient;
    init();
  }

  private void init() throws DocumentClientException {
    jsonDocColumnIndexInCsv = Settings.getJsonDocColumnIndexInCsv();
    this.partitionId = this.importWorkItem.getJsonStoreEntityInstance().partitionId;
    List<ImportWorkItem> foundPartitionWorkItems =
        new ImportQueue(this.importTrackingClient).getPartitionImportWorkItems(this.partitionId);

    partitionImportWorkItems = new ImportWorkItem[foundPartitionWorkItems.size()];
    partitionImportWorkItems = foundPartitionWorkItems.toArray(partitionImportWorkItems);

    this.sortedPartitionImportWorkItems = new ArrayList<>();

    // Note, all sorted files have defined pattern and numbered sequentially
    // Since Azure data lake won't guarantee the order we need to order the file before we insert
    // for more details please see usql script generation.
    int orderCount = 0;

    // take first element to build the sample string
    String adlFileLocation =
        partitionImportWorkItems[orderCount].getJsonStoreEntityInstance().location;

    String expectedAdlFilePrefix = adlFileLocation.substring(0, adlFileLocation.lastIndexOf("_"));

    while (orderCount <= Settings.getEstimatedNumberOfFilesInPartition()) {
      String expectedAdlFile =
          MessageFormat.format("{0}_{1}.json", expectedAdlFilePrefix, orderCount);

      Optional<ImportWorkItem> optional =
          Arrays.stream(partitionImportWorkItems)
              .filter(x -> expectedAdlFile.equals(x.getJsonStoreEntityInstance().name))
              .findFirst();

      if (optional.isPresent()) {
        this.sortedPartitionImportWorkItems.add(optional.get());
      }
      orderCount++;
    }
  }

  @Override
  public void run() {

    if (sortedPartitionImportWorkItems.size() <= 0) {
      return;
    }

    boolean isError = false;

    AdlFileReaderRunnable currentReaderTask = null;
    AdlFileReaderRunnable nextReaderTask = null;

    try {
      currentReaderTask =
          new AdlFileReaderRunnable(
              sortedPartitionImportWorkItems.get(0).getJsonStoreEntityInstance(),
              jsonDocColumnIndexInCsv);
      Thread currentReaderThread = new Thread(currentReaderTask);
      currentReaderThread.start();

      // When more than one item start 2 file also in the background so
      // there won't be delay in cosmos db ingestion in between batches.
      if (sortedPartitionImportWorkItems.size() > 1) {
        nextReaderTask =
            new AdlFileReaderRunnable(
                sortedPartitionImportWorkItems.get(1).getJsonStoreEntityInstance(),
                jsonDocColumnIndexInCsv);

        Thread nextReaderThread = new Thread(nextReaderTask);
        nextReaderThread.start();
      }

    } catch (IOException e) {
      isError = true;
      e.printStackTrace();
    }

    if (isError) {
      logger.error("Failed to create reader tasks.");
      return;
    }

    int importWorkItemsTrackingCounter = 0;
    while (importWorkItemsTrackingCounter < sortedPartitionImportWorkItems.size()) {
      jsonStoreEntityImportResponses = new ArrayList<>();
      while (!currentReaderTask.getIsCompleted()) {
        try {
          List<String> docs = currentReaderTask.getJsonDocs();
          if (docs != null) {
            logger.info("Received data, starting bulk import");

            try {
              BulkImportResponse response =
                  bulkImporter.importAll(docs, true, true, null);
              jsonStoreEntityImportResponses.add(
                  new JsonStoreEntityImportResponse(response, docs.size()));
              logger.info("P_" + partitionId + " Total time " + response.getTotalTimeTaken());
              logger.info("P_" + partitionId + " Documents sent " + docs.size());
              logger.info(
                  "P_"
                      + partitionId
                      + " Documents imported "
                      + response.getNumberOfDocumentsImported());
              logger.info(
                  "P_"
                      + partitionId
                      + " Total Rus consumed "
                      + response.getTotalRequestUnitsConsumed());
            } catch (DocumentClientException e) {
              e.printStackTrace();
            }
          } else {
            // waiting for documents
            Thread.sleep(300);
            // logger.info("waiting for documents from adl");
          }
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
      JsonStoreEntityImportResponse aggregateResponse =
          JsonStoreEntityImportResponse.aggregate(jsonStoreEntityImportResponses);
      // Update the json entity
      JsonStoreEntity entity =
          sortedPartitionImportWorkItems
              .get(importWorkItemsTrackingCounter)
              .getJsonStoreEntityInstance();
      entity.jsonStoreEntityImportResponse = aggregateResponse;
      entity.isComplete = true;
      entity.isInProgress = false;
      entity.completeDate = Instant.now();
      try {
        this.importTrackingClient.updateItem(
            sortedPartitionImportWorkItems
                .get(importWorkItemsTrackingCounter)
                .getMappedCosmosDbDocument(),
            gson.toJson(entity));
      } catch (DocumentClientException e) {
        e.printStackTrace();
      }

      importWorkItemsTrackingCounter++;

      currentReaderTask = nextReaderTask;

      // if next item exists
      if (importWorkItemsTrackingCounter + 1 < sortedPartitionImportWorkItems.size()) {
        try {
          nextReaderTask =
              new AdlFileReaderRunnable(
                  sortedPartitionImportWorkItems
                      .get(importWorkItemsTrackingCounter + 1)
                      .getJsonStoreEntityInstance(),
                  jsonDocColumnIndexInCsv);
          Thread nextReaderThread = new Thread(nextReaderTask);
          nextReaderThread.start();

        } catch (IOException e) {
          isError = true;
          e.printStackTrace();
        }

        if (isError) {
          logger.error("Failed to create reader tasks.");
          return;
        }
      }
    }
  }

  private String getPartitionId(String fileName) {
    String[] parts = fileName.split(Pattern.quote("."));
    String[] firstPartSplits = parts[0].split("_");
    return firstPartSplits[firstPartSplits.length - 1];
  }
}
