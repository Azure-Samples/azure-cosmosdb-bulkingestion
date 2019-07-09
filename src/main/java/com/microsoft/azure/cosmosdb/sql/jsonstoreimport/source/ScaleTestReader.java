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

package com.microsoft.azure.cosmosdb.sql.jsonstoreimport.source;

import com.microsoft.azure.cosmosdb.sql.jsonstoreimport.Settings;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import org.apache.log4j.Logger;

public class ScaleTestReader {
  private static final Logger logger = Logger.getLogger(ScaleTestReader.class);
  private static Queue<ArrayList<String>> preCookedData = new LinkedList<>();

  private static ArrayList<String> prepareJsonTestData() throws Exception {
    List<String> jsonSampleDocs = FileReader.getJsonDocs("template.json");
    if (jsonSampleDocs.size() != 1) {
      throw new Exception("Template should have well formatted json string in a single line.");
    }

    final long startTime = System.currentTimeMillis();
    ArrayList<String> jsonRecords = new ArrayList<>();
    for (int i = 0; i < Settings.getJsonDocsBatchCount(); i++) {
      jsonRecords.add(Settings.applyIdAndPartitionKeySettings(jsonSampleDocs.get(0)).toString());
    }
    logger.info("Total loaded records : " + jsonRecords.size());
    long endTime = System.currentTimeMillis();
    long totalTime = endTime - startTime;
    logger.info("Total records preparation time in seconds: " + totalTime / 1000);
    return jsonRecords;
  }

  public static void preCookData() throws Exception {
    while (preCookedData.size() < Settings.getpreCookedDataQueueSize()) {
      ArrayList<String> jsonRecords = prepareJsonTestData();
      preCookedData.add(jsonRecords);
    }
  }

  static List<String> getJsonTestData() throws Exception {
    if (preCookedData.isEmpty()) {
      logger.info("Queue is empty so cooking my self.");
      return prepareJsonTestData();
    } else {
      logger.info("Returning data from pre cooked data queue");
      return preCookedData.remove();
    }
  }

  static List<String> getJsonPartitionTestData() throws Exception {
    while (preCookedData.isEmpty()) {
      logger.info("waiting for a sec to get data.");
      Thread.sleep(1000);
    }
    logger.info("Returning data from pre cooked data queue");
    return preCookedData.remove();
  }
}
