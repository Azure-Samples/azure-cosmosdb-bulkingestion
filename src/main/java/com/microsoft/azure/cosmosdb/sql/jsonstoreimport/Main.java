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

import com.microsoft.azure.cosmosdb.sql.jsonstoreimport.source.JsonStoreEntity;
import java.io.IOException;
import org.apache.log4j.Logger;

public class Main {

  private static final Logger logger = Logger.getLogger(Main.class);

  public static void main(String[] args) throws Exception {
    CmdLineOptions cmdLineOptions = CmdLineOptions.init(args);
    Settings.init(cmdLineOptions);
    logger.info(cmdLineOptions);

    // Step1: Queue files to cosmos-db
    if (cmdLineOptions.getQueue() != null) {
      prepareCosmosDBQueue(cmdLineOptions);
      System.exit(0);
    }

    // Step2: Import Local/ADL/AzureBlob file or queued files in Cosmos-DB
    if (cmdLineOptions.getIngestionFrom() != null) {
      importData(cmdLineOptions);
      System.exit(0);
    }

    // Usql scripts for shuffling and sorting data by partition
    if (cmdLineOptions.getUsql() != null) {
      prepareUsqlJobs(cmdLineOptions);
      System.exit(0);
    }

    if (cmdLineOptions.getTestData() != null) {

      generateTestData(cmdLineOptions);

      System.exit(0);
    }

    CmdLineOptions.help();

    System.exit(0);
  }

  static void prepareCosmosDBQueue(CmdLineOptions cmdLineOptions) throws Exception {
    String storeType = cmdLineOptions.getStoreType().toLowerCase();
    if (storeType.equals(Constants.ADL)) {

      if (cmdLineOptions.getIsSorted()) {
        new ImportExecutor(true)
            .queueItemsByPartition(
                StoreType.ADL, cmdLineOptions.getQueue(), Constants.OPERATION_PARTITIONED_WRITES);
      } else {
        new ImportExecutor(true)
            .queueItems(
                StoreType.ADL, cmdLineOptions.getQueue(), Constants.OPERATION_UNPARTITIONED_WRITES);
      }

    } else if (storeType.equals(Constants.AZURE_BLOB)) {
      new ImportExecutor(true)
          .queueItems(
              StoreType.AZURE_BLOB,
              cmdLineOptions.getQueue(),
              Constants.OPERATION_UNPARTITIONED_WRITES);
    }
  }

  static void importData(CmdLineOptions cmdLineOptions) throws Exception {
    String ingestionFrom = cmdLineOptions.getIngestionFrom().toLowerCase();
    switch (ingestionFrom) {
      case Constants.LOCAL:
        if (cmdLineOptions.getIngestionFilePath() != null) {
          JsonStoreEntity jsonStoreEntity = new JsonStoreEntity();
          jsonStoreEntity.storeType = StoreType.WINDOWS_FILE_SYSTEM;
          jsonStoreEntity.location = cmdLineOptions.getIngestionFilePath();
          new ImportExecutor(false).processJsonStoreEntity(jsonStoreEntity);
          return;
        }
      case Constants.ADL:
        if (cmdLineOptions.getIngestionFilePath() != null) {
          JsonStoreEntity jsonStoreEntity = new JsonStoreEntity();
          jsonStoreEntity.storeType = StoreType.ADL;
          jsonStoreEntity.location = cmdLineOptions.getIngestionFilePath();
          new ImportExecutor(false).processJsonStoreEntity(jsonStoreEntity);
          return;
        }
      case Constants.AZURE_BLOB:
        if (cmdLineOptions.getIngestionFilePath() != null) {
          JsonStoreEntity jsonStoreEntity = new JsonStoreEntity();
          jsonStoreEntity.storeType = StoreType.AZURE_BLOB;
          jsonStoreEntity.location = cmdLineOptions.getIngestionFilePath();
          new ImportExecutor(false).processJsonStoreEntity(jsonStoreEntity);
          return;
        }
      case Constants.COSMOS_DB:
        if (cmdLineOptions.getIsSorted()) {
          new ImportExecutor(true).processSortedDataByPartition();
        } else {
          new ImportExecutor(true).processQueue();
        }
        return;
      case Constants.SCALE_TEST:
        new ImportExecutor(true).initScaleTest();
        return;

      default:
        throw new Exception(
            "Option "
                + ingestionFrom
                + " not supported. please look at user guides "
                + "in docs folder.");
    }
  }

  static void prepareUsqlJobs(CmdLineOptions cmdLineOptions) throws IOException {
    if (cmdLineOptions.getUsql().toLowerCase().equals(Constants.CREATE_USQL_SCRIPTS)) {
      UsqlHelper.generateUsqlScripts();
    } else if (cmdLineOptions.getUsql().toLowerCase().equals(Constants.SUBMIT_USQL_SCRIPTS)) {
      if (cmdLineOptions.getIsShufflePhase()) {
        UsqlHelper.submitShuffleJobs();
      } else if (cmdLineOptions.getIsSortPhase()) {
        UsqlHelper.submitSortJobs();
      }
    }
  }

  static void generateTestData(CmdLineOptions cmdLineOptions) throws IOException {
    if (cmdLineOptions.getTestData().toLowerCase().equals(Constants.ADL)) {

      new TestDataGenerator().generateDummyFiles();
    }
  }
}
