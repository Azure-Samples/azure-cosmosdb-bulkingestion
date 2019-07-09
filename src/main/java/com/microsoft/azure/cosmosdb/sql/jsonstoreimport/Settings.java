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

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.UUID;
import org.json.JSONObject;

public class Settings {

  // Azure Blob Settings
  private static String azureBlobConnectionString;

  // ADL Settings
  private static String AdlAccountFQDN;
  private static String AdlClientId;
  private static String AdlAuthTokenEndpoint;
  private static String AdlClientKey;
  private static int maxFileEntriesToRetrieve;
  private static String adlShuffleFolder;
  private static String adlSortedDataFolder;
  private static String adlSourceFolder;
  // CosmosDB Settings
  private static String CosmosDbEndPoint;
  private static String CosmosDbMasterKey;
  private static String cosmosDbDatabase;
  private static String cosmosDbImportTrackingCollection;
  private static int importTrackingCollectionThroughput;
  private static String cosmosDbDataCollection;
  private static int cosmosDbDataCollectionThroughput;
  private static String cosmosDbDataCollectionPkValue;
  private static int cosmosDbImportMiniBatchMaxSizeInBytes;
  private static int cosmosDbDataCollectionConnectionPoolSize;
  private static int cosmosDbBulkImportLibBatchSize;
  // Settings for mapping keys between input json data and cosmos db document
  private static String idField;
  private static boolean useGuidForId;
  private static boolean UseGuidForPk;
  // Scale test settings
  private static int jsonDocsBatchCount;
  private static String runTag;
  private static int preCookedDataQueueSize;

  // USql shuffle settings
  private static String adlaAccountName;
  private static String adlaTenantId;
  private static String adlaSubId;
  private static String adlaClientId;
  private static String adlaClientSecret;
  private static String shuffleUsqlScriptOutputFolder;
  private static String sortUsqlScriptOutputFolder;
  private static int numberOfUsqlJobs;
  private static int numberOfSourceDataFilesToProcess;
  private static int numberOfCosmosDbPartitions;
  // Partitions processing settings
  private static int partitionsLimitForWorker;
  private static boolean mergePartitions;
  private static int dummyFilesCount;
  private static String dummyFilePath;
  private static int estimatedNumberOfFilesInPartition;
  private static int jsonDocColumnIndexInCsv;

  public static String getAdlaAccountName() {
    return adlaAccountName;
  }

  public static String getAdlaTenantId() {
    return adlaTenantId;
  }

  public static String getAdlaSubId() {
    return adlaSubId;
  }

  public static String getAdlaClientId() {
    return adlaClientId;
  }

  public static String getAdlaClientSecret() {
    return adlaClientSecret;
  }

  public static int getJsonDocColumnIndexInCsv() {
    return jsonDocColumnIndexInCsv;
  }

  public static int getEstimatedNumberOfFilesInPartition() {
    return estimatedNumberOfFilesInPartition;
  }

  public static String getAdlShuffleFolder() {
    return adlShuffleFolder;
  }

  public static String getAdlSortedDataFolder() {
    return adlSortedDataFolder;
  }

  public static String getAdlSourceFolder() {
    return adlSourceFolder;
  }

  public static String getShuffleUsqlScriptOutputFolder() {
    return shuffleUsqlScriptOutputFolder;
  }

  public static String getSortUsqlScriptOutputFolder() {
    return sortUsqlScriptOutputFolder;
  }

  public static int getNumberOfUsqlJobs() {
    return numberOfUsqlJobs;
  }

  public static int getNumberOfSourceDataFilesToProcess() {
    return numberOfSourceDataFilesToProcess;
  }

  public static int getNumberOfCosmosDbPartitions() {
    return numberOfCosmosDbPartitions;
  }

  public static int getPartitionsLimitForWorker() {
    return partitionsLimitForWorker;
  }

  public static int getDummyFilesCount() {
    return dummyFilesCount;
  }

  public static String getDummyFilePath() {
    return dummyFilePath;
  }

  public static String getAdlAccountFQDN() {
    return AdlAccountFQDN;
  }

  public static String getAdlClientId() {
    return AdlClientId;
  }

  public static String getAdlAuthTokenEndpoint() {
    return AdlAuthTokenEndpoint;
  }

  public static String getAdlClientKey() {
    return AdlClientKey;
  }

  public static int getMaxFileEntriesToRetrieve() {
    return maxFileEntriesToRetrieve;
  }

  public static String getAzureBlobConnectionString() {
    return azureBlobConnectionString;
  }

  public static int getCosmosDbDataCollectionConnectionPoolSize() {
    return cosmosDbDataCollectionConnectionPoolSize;
  }

  public static int getCosmosDbBulkImportLibBatchSize() {
    return cosmosDbBulkImportLibBatchSize;
  }

  public static JSONObject applyIdAndPartitionKeySettings(String line) {
    JSONObject object = new JSONObject(line);
    if (Settings.idField != null && !Settings.idField.isEmpty()) {
      object.put("id", object.get(Settings.idField));
    }
    if (Settings.cosmosDbDataCollectionPkValue != null
        && !Settings.cosmosDbDataCollectionPkValue.isEmpty()) {
      object.put(
          Settings.cosmosDbDataCollectionPkValue,
          object.get(Settings.cosmosDbDataCollectionPkValue));
    }
    if (Settings.useGuidForId) {
      object.put("id", UUID.randomUUID().toString());
    }
    if (Settings.UseGuidForPk) {
      object.put(Settings.cosmosDbDataCollectionPkValue, UUID.randomUUID().toString());
    }
    return object;
  }

  public static int getJsonDocsBatchCount() {
    return jsonDocsBatchCount;
  }

  public static int getpreCookedDataQueueSize() {
    return preCookedDataQueueSize;
  }

  static void initSettings() throws IOException {
    Properties settings = new Properties();
    InputStream propertiesInputStream =
        Main.class.getClassLoader().getResourceAsStream("settings.properties");
    settings.load(propertiesInputStream);
    registerSettingValues(settings);
  }

  static String getCosmosDbEndPoint() {
    return CosmosDbEndPoint;
  }

  static String getCosmosDbMasterKey() {
    return CosmosDbMasterKey;
  }

  static String getCosmosDbDatabase() {
    return cosmosDbDatabase;
  }

  static String getCosmosDbDataCollection() {
    return cosmosDbDataCollection;
  }

  static String getCosmosDbImportTrackingCollection() {
    return cosmosDbImportTrackingCollection;
  }

  static int getImportTrackingCollectionThroughput() {
    return importTrackingCollectionThroughput;
  }

  static int getCosmosDbDataCollectionThroughput() {
    return cosmosDbDataCollectionThroughput;
  }

  static String getCosmosDbDataCollectionPkValue() {
    return cosmosDbDataCollectionPkValue;
  }

  static int getCosmosDbImportMiniBatchMaxSizeInBytes() {
    return cosmosDbImportMiniBatchMaxSizeInBytes;
  }

  static String getRunTag() {
    return runTag;
  }

  static void initUserSettings(String filePath) throws IOException {
    Properties settings = new Properties();
    File f = new File(filePath);
    InputStream targetStream = new FileInputStream(f);
    settings.load(targetStream);
    targetStream.close();
    registerSettingValues(settings);
  }

  static void init(CmdLineOptions cmdLineOptions) throws IOException {
    if (cmdLineOptions.getConfFile() != null) {
      Settings.initUserSettings(cmdLineOptions.getConfFile());
    } else {
      Settings.initSettings();
    }
  }

  private static void registerSettingValues(Properties settings) {
    // Azure Blob Settings
    Settings.azureBlobConnectionString = settings.getProperty("azureBlobConnectionString");

    // ADL Settings
    Settings.AdlAccountFQDN = settings.getProperty("adlAccountFQDN");
    Settings.AdlClientId = settings.getProperty("adlClientId");
    Settings.AdlAuthTokenEndpoint = settings.getProperty("adlAuthTokenEndpoint");
    Settings.AdlClientKey = settings.getProperty("adlClientKey");
    Settings.maxFileEntriesToRetrieve =
        parseOrDefault(settings.getProperty("maxFileEntriesToRetrieve"), 100000);
    Settings.adlShuffleFolder = settings.getProperty("adlShuffleFolder");
    Settings.adlSortedDataFolder = settings.getProperty("adlSortedDataFolder");
    Settings.adlSourceFolder = settings.getProperty("adlSourceFolder");

    // CosmosDB Settings
    Settings.CosmosDbEndPoint = settings.getProperty("cosmosDbEndPoint");
    Settings.CosmosDbMasterKey = settings.getProperty("cosmosDbMasterkey");
    Settings.cosmosDbDatabase = settings.getProperty("cosmosDbDatabase");

    Settings.cosmosDbImportTrackingCollection =
        settings.getProperty("cosmosDbImportTrackingCollection");
    Settings.importTrackingCollectionThroughput =
        parseOrDefault(settings.getProperty("importTrackingCollectionThroughput"), -1);

    Settings.cosmosDbDataCollection = settings.getProperty("cosmosDbDataCollection");
    Settings.cosmosDbDataCollectionThroughput =
        parseOrDefault(settings.getProperty("cosmosDbDataCollectionThroughput"), -1);
    Settings.cosmosDbDataCollectionPkValue = settings.getProperty("cosmosDbDataCollectionPkValue");

    Settings.cosmosDbImportMiniBatchMaxSizeInBytes =
        parseOrDefault(settings.getProperty("cosmosDbImportMiniBatchMaxSizeInBytes"), -1);
    Settings.cosmosDbDataCollectionConnectionPoolSize =
        parseOrDefault(settings.getProperty("cosmosDbDataCollectionConnectionPoolSize"), 200);

    Settings.cosmosDbBulkImportLibBatchSize =
        parseOrDefault(settings.getProperty("cosmosDbBulkImportLibBatchSize"), -1);

    Settings.idField = settings.getProperty("idField");
    Settings.useGuidForId = Boolean.parseBoolean(settings.getProperty("useGuidForId"));
    Settings.UseGuidForPk = Boolean.parseBoolean(settings.getProperty("useGuidForPk"));

    // Scale test settings
    Settings.jsonDocsBatchCount =
        parseOrDefault(settings.getProperty("jsonDocsBatchCount"), 800000);
    Settings.runTag = settings.getProperty("runTag");
    Settings.preCookedDataQueueSize =
        parseOrDefault(settings.getProperty("preCookedDataQueueSize"), 3);

    // Partitions processing settings
    Settings.partitionsLimitForWorker =
        parseOrDefault(settings.getProperty("partitionsLimitForWorker"), -1);
    Settings.mergePartitions = Boolean.parseBoolean(settings.getProperty("mergePartitions"));

    Settings.dummyFilesCount = parseOrDefault(settings.getProperty("dummyFilesCount"), -1);
    Settings.dummyFilePath = settings.getProperty("dummyFilePath");

    // USql shuffle settings

    Settings.adlaAccountName = settings.getProperty("adlaAccountName");
    Settings.adlaTenantId = settings.getProperty("adlaTenantId");
    Settings.adlaSubId = settings.getProperty("adlaSubId");
    Settings.adlaClientId = settings.getProperty("adlaClientId");
    Settings.adlaClientSecret = settings.getProperty("adlaClientSecret");

    Settings.shuffleUsqlScriptOutputFolder = settings.getProperty("shuffleUsqlScriptOutputFolder");
    Settings.sortUsqlScriptOutputFolder = settings.getProperty("sortUsqlScriptOutputFolder");
    Settings.numberOfUsqlJobs = parseOrDefault(settings.getProperty("numberOfUsqlJobs"), -1);
    Settings.numberOfSourceDataFilesToProcess =
        parseOrDefault(settings.getProperty("numberOfSourceDataFilesToProcess"), -1);
    Settings.numberOfCosmosDbPartitions =
        parseOrDefault(settings.getProperty("numberOfCosmosDbPartitions"), -1);
    Settings.estimatedNumberOfFilesInPartition =
        parseOrDefault(settings.getProperty("estimatedNumberOfFilesInPartition"), -1);
    Settings.jsonDocColumnIndexInCsv =
        parseOrDefault(settings.getProperty("jsonDocColumnIndexInCsv"), -1);
  }

  private static int parseOrDefault(String value, int defaultValue) {
    if (value == null) {
      return defaultValue;
    }
    return Integer.parseInt(value);
  }
}
