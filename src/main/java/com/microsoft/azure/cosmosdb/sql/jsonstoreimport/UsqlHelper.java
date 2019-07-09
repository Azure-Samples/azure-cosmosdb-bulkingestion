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
import com.microsoft.azure.cosmosdb.sql.jsonstoreimport.sdkextensions.AdlStoreClientExtension;
import com.microsoft.azure.credentials.ApplicationTokenCredentials;
import com.microsoft.azure.management.datalake.analytics.DataLakeAnalyticsJobManagementClient;
import com.microsoft.azure.management.datalake.analytics.implementation.DataLakeAnalyticsJobManagementClientImpl;
import com.microsoft.azure.management.datalake.analytics.models.CreateJobParameters;
import com.microsoft.azure.management.datalake.analytics.models.CreateUSqlJobProperties;
import com.microsoft.azure.management.datalake.analytics.models.JobInformation;
import com.microsoft.azure.management.datalake.analytics.models.JobType;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.apache.log4j.Logger;

public class UsqlHelper {
  private static final Logger logger = Logger.getLogger(UsqlHelper.class);

  private static final String _adlaAccountName = Settings.getAdlaAccountName();
  private static final String _tenantId = Settings.getAdlaTenantId();
  private static final String _clientId = Settings.getAdlClientId();
  private static final String _clientSecret = Settings.getAdlaClientSecret();

  private static final String adlAccount =
      MessageFormat.format("adl://{0}", Settings.getAdlAccountFQDN());
  private static final String adlSourceDataFolder = Settings.getAdlSourceFolder();
  private static final String adlShuffleDataFolder = Settings.getAdlShuffleFolder();
  private static final String adlSortDataFolder = Settings.getAdlSortedDataFolder();

  private static final String shuffleUsqlScriptOutputFolder =
      Settings.getShuffleUsqlScriptOutputFolder();
  private static final String sortUsqlScriptOutputFolder = Settings.getSortUsqlScriptOutputFolder();
  private static final int numberOfUsqlJobs = Settings.getNumberOfUsqlJobs();
  private static final int numberOfCosmosDbPartitions = Settings.getNumberOfCosmosDbPartitions();
  private static final int numberOfSourceDataFilesToProcess =
      Settings.getNumberOfSourceDataFilesToProcess();

  static void generateUsqlScripts() throws IOException {

    generateUsqlShuffleScripts();
    generateUsqlSortScripts();
  }

  static void generateUsqlShuffleScripts() throws IOException {

    List<String> files = new ArrayList<>();
    List<String> filesFromAdl = AdlStoreClientExtension.getAdlFolderFileNames(adlSourceDataFolder);
    int totalFiles = filesFromAdl.size();
    if (totalFiles > numberOfSourceDataFilesToProcess) {
      totalFiles = numberOfSourceDataFilesToProcess;
    }

    filesFromAdl.sort((a, b) -> a.compareTo(b));

    for (int i = 0; i < totalFiles; i++) {
      files.add(adlAccount + filesFromAdl.get(i));
    }

    List<List<String>> batches = Lists.partition(files, totalFiles / numberOfUsqlJobs);
    int batchNumber = 0;
    File shuffleFolder = new File(shuffleUsqlScriptOutputFolder);
    if (!shuffleFolder.exists()) {
      shuffleFolder.mkdir();
    }
    for (List<String> batch : batches) {
      File file =
          new File(shuffleUsqlScriptOutputFolder + "\\Shuffle_Batch_" + batchNumber + ".usql");
      file.createNewFile();
      FileWriter writer = new FileWriter(file);

      StringBuilder filesList = new StringBuilder();
      for (int i = 0; i < batch.size(); i++) {
        if (i < batch.size() - 1) {
          filesList.append("\"" + batch.get(i) + "\"");
          filesList.append(",");
        } else {
          filesList.append("\"" + batch.get(i) + "\"");
        }
      }

      writer.write(
          "REFERENCE ASSEMBLY [Newtonsoft.Json];\n"
              + "REFERENCE ASSEMBLY [Microsoft.Analytics.Samples.Formats];\n"
              + "REFERENCE ASSEMBLY [Microsoft.Analytics.CosmosDB.Util];\n"
              + "REFERENCE ASSEMBLY [Microsoft.Azure.Documents.Client];\n"
              + "\n"
              + "USING Microsoft.Analytics.Samples.Formats.Json;\n"
              + "USING Microsoft.Analytics.CosmosDB.Util;\n");

      writer.write(
          "@source_raw_json_docs =\n" + "    EXTRACT jsonString string // limited to " + "128kB\n");
      writer.write("    FROM " + filesList.toString() + "\n");

      writer.write("    USING Extractors.Text(delimiter : '\\b', quoting : false);\n");

      writer.write(
          "@parsed_json_docs = SELECT JsonFunctions.JsonTuple(jsonString, "
              + "\"..*\") AS jsonDoc, jsonString AS payLoad FROM @source_raw_json_docs;\n");

      writer.write(
          "@json_docs_with_hash_key = SELECT \n"
              + "            payLoad,\n"
              + "            EffectivePartitionKeyGenerator.GetEffectivePartitionKey(jsonDoc[\"id\"],payLoad,\"/key\") AS hashKey,\n"
              + "EffectivePartitionKeyGenerator.GetPartitionKeyByEffectivePartitionKey(EffectivePartitionKeyGenerator.GetEffectivePartitionKey(jsonDoc[\"id\"],payLoad,\"/key\")) AS partitionId\n"
              + " FROM @parsed_json_docs;\n");

      for (int i = 0; i < numberOfCosmosDbPartitions; i++) {
        writer.write(
            "@partition_id_"
                + i
                + "=SELECT hashKey,payLoad FROM @json_docs_with_hash_key WHERE "
                + "partitionId==\""
                + i
                + "\";\n");

        writer.write(
            "OUTPUT @partition_id_"
                + i
                + "\n"
                + "TO "
                + "\""
                + adlAccount
                + adlShuffleDataFolder
                + "partition_id_"
                + i
                + "/batch_"
                + batchNumber
                + ".json\"\n"
                + "USING Outputters.Csv();\n");
      }

      batchNumber++;

      writer.flush();
      writer.close();
    }
  }

  static void generateUsqlSortScripts() throws IOException {

    List<String> partitionsList = new ArrayList<>();
    for (int i = 0; i < numberOfCosmosDbPartitions; i++) {
      partitionsList.add(Integer.toString(i));
    }

    List<List<String>> partitionBatches =
        Lists.partition(partitionsList, numberOfCosmosDbPartitions / numberOfUsqlJobs);

    int partitionsBatchNumber = 0;

    String shuffleFileFormatString =
        "\"" + adlAccount + adlShuffleDataFolder + "partition_id_{0}/'{'*'}'" + ".json" + "\"";

    File sortFolder = new File(sortUsqlScriptOutputFolder);
    if (!sortFolder.exists()) {
      sortFolder.mkdir();
    }
    for (List<String> partitionBatch : partitionBatches) {
      partitionsBatchNumber++;
      File file =
          new File(
              sortUsqlScriptOutputFolder + "\\Sort_Batch_" + partitionsBatchNumber + "" + ".usql");
      file.createNewFile();
      FileWriter writer = new FileWriter(file);

      writer.write(
          "REFERENCE ASSEMBLY [Newtonsoft.Json];\n"
              + "REFERENCE ASSEMBLY [Microsoft.Analytics.Samples.Formats];\n"
              + "REFERENCE ASSEMBLY [Microsoft.Analytics.CosmosDB.Util];\n"
              + "REFERENCE ASSEMBLY [Microsoft.Azure.Documents.Client];\n"
              + "\n"
              + "USING Microsoft.Analytics.Samples.Formats.Json;\n"
              + "USING Microsoft.Analytics.CosmosDB.Util;");

      for (String p : partitionBatch) {

        writer.write(
            "\n"
                + "@files_input_partition_id_"
                + p
                + " =\n"
                + "EXTRACT \n"
                + "hashKey string,\n"
                + "payLoad string\n");
        writer.write("    FROM " + MessageFormat.format(shuffleFileFormatString, p));
        writer.write(System.getProperty("line.separator"));
        writer.write("    USING Extractors.Csv();\n");
        writer.write(System.getProperty("line.separator"));

        writer.write(
            "@sorted_partition_id_"
                + p
                + "=SELECT ROW_NUMBER() OVER (ORDER BY hashKey "
                + "ASC) AS rowNumber,\n"
                + "   payLoad, hashKey\n"
                + "    FROM @files_input_partition_id_"
                + p
                + ";\n");
        writer.write(System.getProperty("line.separator"));

        int oneMbRecords = 4000;
        int expectedFileSizeInMb = 500;
        int expectedPartitionSizeInMb = 8000;
        int start = 1;
        int end = -1;
        int numberOfFiles = 0;
        while (end < expectedPartitionSizeInMb * oneMbRecords) {
          numberOfFiles++;
          end = start + (expectedFileSizeInMb * oneMbRecords) - 1;
          writer.write(
              "@sorted_partition_id_"
                  + p
                  + "_part_"
                  + numberOfFiles
                  + " =\n"
                  + "    "
                  + " SELECT * FROM @sorted_partition_id_"
                  + p
                  + "\n"
                  + " WHERE rowNumber >= "
                  + start
                  + " AND rowNumber <= "
                  + end
                  + ";\n");
          // System.out.println("Start " + start + " end " + end);
          start = end + 1;
          writer.write(System.getProperty("line.separator"));
          writer.write(
              "OUTPUT @sorted_partition_id_"
                  + p
                  + "_part_"
                  + numberOfFiles
                  + "\nTO \""
                  + adlAccount
                  + adlSortDataFolder
                  + "partition_id_"
                  + p
                  + "/batch_"
                  + partitionsBatchNumber
                  + "_part_"
                  + numberOfFiles
                  + ".json\"\n"
                  + " ORDER BY hashKey ASC \n"
                  + "USING Outputters.Text();\n");
          writer.write(System.getProperty("line.separator"));
        }
        // Just move anything more than expected to one file
        numberOfFiles++;
        writer.write(
            "@sorted_partition_id_"
                + p
                + "_part_"
                + numberOfFiles
                + " =\n"
                + " SELECT * FROM @sorted_partition_id_"
                + p
                + "\n"
                + " WHERE rowNumber >= "
                + end
                + ";");
        writer.write(System.getProperty("line.separator"));
        writer.write(
            "OUTPUT @sorted_partition_id_"
                + p
                + "_part_"
                + numberOfFiles
                + "\nTO \""
                + adlAccount
                + adlSortDataFolder
                + "partition_id_"
                + p
                + "/batch_"
                + partitionsBatchNumber
                + "_part_"
                + numberOfFiles
                + ".json\"\n"
                + "ORDER BY hashKey ASC \n"
                + "USING Outputters.Text();");
        writer.write(System.getProperty("line.separator"));
      }
      writer.flush();
      writer.close();
    }
  }

  static void submitShuffleJobs() {
    submitUsqlJobs(shuffleUsqlScriptOutputFolder);
  }

  static void submitUsqlJobs(String scriptsOutputFolder) {
    DataLakeAnalyticsJobManagementClient _adlaJobClient;
    ApplicationTokenCredentials creds =
        new ApplicationTokenCredentials(_clientId, _tenantId, _clientSecret, null);

    _adlaJobClient = new DataLakeAnalyticsJobManagementClientImpl(creds);

    final File folder = new File(scriptsOutputFolder);
    for (final File fileEntry : folder.listFiles()) {

      StringBuilder contentBuilder = new StringBuilder();
      try (BufferedReader br = new BufferedReader(new java.io.FileReader(fileEntry))) {

        String sCurrentLine;
        while ((sCurrentLine = br.readLine()) != null) {
          contentBuilder.append(sCurrentLine).append("\n");
        }
      } catch (IOException e) {
        e.printStackTrace();
      }
      String script = contentBuilder.toString();

      UUID jobId = java.util.UUID.randomUUID();

      CreateJobParameters createJobParameters = new CreateJobParameters();
      createJobParameters.withDegreeOfParallelism(10);
      createJobParameters.withType(JobType.USQL);
      String jobName = fileEntry.getName().replace(".usql", "");
      logger.info("Submitting usql job with name: " + jobName);
      createJobParameters.withName(jobName);
      CreateUSqlJobProperties usqljobProperties = new CreateUSqlJobProperties();
      usqljobProperties.withScript(script);
      createJobParameters.withProperties(usqljobProperties);

      JobInformation jobInformation =
          _adlaJobClient.jobs().create(_adlaAccountName, jobId, createJobParameters);
    }
  }

  static void submitSortJobs() {
    submitUsqlJobs(sortUsqlScriptOutputFolder);
  }
}
