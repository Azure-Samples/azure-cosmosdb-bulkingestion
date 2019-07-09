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
import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.log4j.Logger;

public class FileReader {
  private static final Logger logger = Logger.getLogger(FileReader.class);

  public static List<String> getJsonDocs(String filePath, Boolean isDelete) throws IOException {
    List<String> jsonRecords = getJsonDocs(filePath);
    if (isDelete) {
      File downloadedFile = new File(filePath);
      logger.info("Deleting downloaded file: " + downloadedFile.getAbsolutePath());
      boolean isDeleted = downloadedFile.delete();
      if (!isDeleted) {
        logger.info("Deletion failed, looks like you don't have permission");
      }
    }
    return jsonRecords;
  }

  public static List<String> getJsonDocs(String filePath) throws IOException {
    logger.info("Loading local file " + filePath + " ...");
    final long startTime = System.currentTimeMillis();
    java.io.FileReader fr = new java.io.FileReader(filePath);
    BufferedReader br = new BufferedReader(fr, 8192);
    String line;
    List<String> jsonRecords = new ArrayList<String>();
    while ((line = br.readLine()) != null) {
      jsonRecords.add(Settings.applyIdAndPartitionKeySettings(line).toString());
    }
    br.close();
    if (fr != null) {
      fr.close();
    }
    logger.info("Total loaded records : " + jsonRecords.size());
    long endTime = System.currentTimeMillis();
    long totalTime = endTime - startTime;
    logger.info("File loading execution time in seconds: " + totalTime / 1000);
    return jsonRecords;
  }

}
