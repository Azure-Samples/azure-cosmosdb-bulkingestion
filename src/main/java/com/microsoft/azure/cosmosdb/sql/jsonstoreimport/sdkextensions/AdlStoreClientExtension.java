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

package com.microsoft.azure.cosmosdb.sql.jsonstoreimport.sdkextensions;

import com.microsoft.azure.cosmosdb.sql.jsonstoreimport.Settings;
import com.microsoft.azure.datalake.store.ADLStoreClient;
import com.microsoft.azure.datalake.store.DirectoryEntry;
import com.microsoft.azure.datalake.store.IfExists;
import com.microsoft.azure.datalake.store.oauth2.AccessTokenProvider;
import com.microsoft.azure.datalake.store.oauth2.ClientCredsTokenProvider;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintStream;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.log4j.Logger;

public class AdlStoreClientExtension {

  private static final Logger logger = Logger.getLogger(AdlStoreClientExtension.class);
  private static ADLStoreClient client;

  public static List<String> getJsonDocs(String fileName) throws IOException {
    logger.info("Loading adl file " + fileName + " ...");
    final long startTime = System.currentTimeMillis();
    List<String> records = new ArrayList<String>();
    initClient();

    try (InputStream in = client.getReadStream(fileName)) {
      try (BufferedReader reader = new BufferedReader(new InputStreamReader(in))) {
        String line;
        while ((line = reader.readLine()) != null) {
          records.add(Settings.applyIdAndPartitionKeySettings(line).toString());
        }
      }
    }
    logger.info("Total loaded records : " + records.size());
    long endTime = System.currentTimeMillis();
    long totalTime = endTime - startTime;
    logger.info("Adl file loading execution time in seconds: " + totalTime / 1000);
    return records;
  }

  private static void initClient() {
    if (client != null) {
      return;
    }
    AccessTokenProvider provider =
        new ClientCredsTokenProvider(
            Settings.getAdlAuthTokenEndpoint().trim(),
            Settings.getAdlClientId(),
            Settings.getAdlClientKey().trim());
    client = ADLStoreClient.createClient(Settings.getAdlAccountFQDN().trim(), provider);
  }

  public static List<String> getAdlFolderFileNames(String adlFolder) throws IOException {
    initClient();
    List<DirectoryEntry> list =
        client.enumerateDirectory(adlFolder, Settings.getMaxFileEntriesToRetrieve());
    List<String> files = new ArrayList<String>();

    logger.info(String.format("Found %d files in ADL folder", list.size()));
    for (DirectoryEntry entry : list) {
      files.add(entry.fullName);
    }
    return files;
  }

  public static void uploadDocs(String fileName, List<String> jsonRecords) throws IOException {
    initClient();
    logger.info("Uploading a file " + fileName);
    OutputStream stream = client.createFile(fileName, IfExists.OVERWRITE);
    PrintStream out = new PrintStream(stream);
    for (int i = 0; i < jsonRecords.size(); i++) {
      out.println(jsonRecords.get(i));
    }
    out.close();
    stream.flush();
    stream.close();
  }

  public static BufferedReader getReadStream(String fileName) throws IOException {
    initClient();
    InputStream in = client.getReadStream(fileName);
    return new BufferedReader(new InputStreamReader(in));
  }

  public static List<String> getJsonDocsFromCsvStream(
      BufferedReader bufferedReader, int noOfRecordsToFetch, int jsonDocColumnIndexInCsv)
      throws IOException {
    int count = 0;
    final long startTime = System.currentTimeMillis();
    List<String> jsonRecords = new ArrayList<>();
    String line;
    while ((line = bufferedReader.readLine()) != null) {
      StringReader stringReader = new StringReader(line);
      CSVParser parser = new CSVParser(stringReader, CSVFormat.DEFAULT);
      String jsonDoc = parser.getRecords().get(0).get(jsonDocColumnIndexInCsv);
      parser.close();
      jsonRecords.add(jsonDoc);
      count++;
      if (count > noOfRecordsToFetch) {
        break;
      }
    }
    long endTime = System.currentTimeMillis();
    long totalTime = endTime - startTime;
    logger.info("Records loading execution time in seconds: " + totalTime / 1000);
    return jsonRecords;
  }

  public static long getFileSize(String fileName) throws IOException {

    DirectoryEntry ent = client.getDirectoryEntry(fileName);
    return ent.length;
  }
}
