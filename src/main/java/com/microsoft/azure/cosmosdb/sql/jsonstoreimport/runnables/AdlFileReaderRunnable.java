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

import com.microsoft.azure.cosmosdb.sql.jsonstoreimport.Settings;
import com.microsoft.azure.cosmosdb.sql.jsonstoreimport.sdkextensions.AdlStoreClientExtension;
import com.microsoft.azure.cosmosdb.sql.jsonstoreimport.source.JsonStoreEntity;
import java.io.BufferedReader;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import org.apache.log4j.Logger;

public class AdlFileReaderRunnable implements Runnable {

  private final Logger logger = Logger.getLogger(AdlFileReaderRunnable.class);
  private Queue<List<String>> preLoadedData = new LinkedList<>();
  private BufferedReader bufferedReader;
  private boolean isCompleted;
  private int waitTime;
  private int jsonDocColumnIndexInCsv;

  private JsonStoreEntity jsonStoreEntity;

  public AdlFileReaderRunnable(JsonStoreEntity jsonStoreEntity, int jsonDocColumnIndexInCsv)
      throws IOException {
    this.jsonStoreEntity = jsonStoreEntity;
    this.bufferedReader = AdlStoreClientExtension.getReadStream(jsonStoreEntity.location);
    this.isCompleted = false;
    this.waitTime = 300;
    this.jsonDocColumnIndexInCsv = jsonDocColumnIndexInCsv;
  }

  public boolean getIsCompleted() {
    return isCompleted && preLoadedData.isEmpty();
  }

  public List<String> getJsonDocs() {

    while (preLoadedData.isEmpty()) {
      return null;
    }
    return preLoadedData.remove();
  }

  @Override
  public void run() {

    while (true) {
      while (preLoadedData.size() < Settings.getpreCookedDataQueueSize()) {

        try {
          final long startTime = System.currentTimeMillis();
          List<String> jsonRecords =
              AdlStoreClientExtension.getJsonDocsFromCsvStream(
                  bufferedReader, Settings.getJsonDocsBatchCount(), this.jsonDocColumnIndexInCsv);
          long endTime = System.currentTimeMillis();
          long totalTime = endTime - startTime;
          logger.info(
              jsonStoreEntity.name
                  + " Fetching records execution time in seconds: "
                  + totalTime / 1000);
          if (jsonRecords.size() <= 0) {
            isCompleted = true;
            logger.info(" File " + jsonStoreEntity.location + " Finished.");
            break;
          } else {
            preLoadedData.add(jsonRecords);
          }

        } catch (IOException e) {
          logger.error("File " + jsonStoreEntity.location + " reading error" + e.getMessage());
        }
      }
      if (isCompleted) {
        logger.info("ADL Reader exiting while loop for " + jsonStoreEntity.name);
        break;
      }
      try {
        // wait for 3 milliseconds
        Thread.sleep(waitTime);
      } catch (InterruptedException e) {
        logger.error("Thread sleep failure" + e.getMessage());
      }
    }
  }
}
