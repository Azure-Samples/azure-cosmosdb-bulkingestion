# Cosmos DB Tool for importing data from Azure Data Lake Store and Azure Blobs
This tool provides the capability to bulk import data from Azure data lake or Azure blob storage into CosmosDB. You should use this tool to import data size larger than 500GB. It maximizes the RU utilization in your CosmosDB collection, providing better performance than a traditional Azure data factory pipeline. Furthermore, if you can't exhaust all RU's with a single instance of the tool, you can run multiple instances on different VM's. At that point, the rate of ingestion will be only limited by the throuhgput you have provisioned for the collection. The tool is equipped to handle syncing in a multi instance configuration to avoid duplication of data.

## Prerequisites:

1. Please setup Azure Data Lake Store and upload your data in chunks, minimum recommended size 200 Mb. [Learn more about Azure Data Lake Store](https://azure.microsoft.com/en-us/services/data-lake-store/) 

2. Setup AAD application to access Data Lake Store from client. [Learn more about AAD registration](https://docs.microsoft.com/en-us/azure/active-directory/active-directory-app-registration)

3. Setup Azure Storage Account and load your data in single container and make sure file names are unique.

4. Setup 2 Cosmos DB Partitioned Collections.

   a. Partitioned collection with more RUs to ingest actual data.

   b. Standard collection for distributing load across multiple workers and for storing the migration status.

> [!NOTE]
> Only account creation is mandatory, rest you can provide in the settings file, tool generates the collections if they don't exist. For help reach out cosmosdbasks@microsoft.com
 ​

## Tool Setup:

1. Generate Jar using Maven: mvn package

2. Update settings.properties using the following guide:

    * Map Id and Partition key columns of Json doc to Cosmos DB Document

        * Map the `key1` column value in input document to Cosmos DB document id column by setting the value below. Empty value ignores this setting

                idField=key1

        * Map `key2` column value in input document to Cosmos DB document partition key column by setting the value below. Empty value ignores this setting

                pkField=key2

        * Set the following boolean variable to true if you have left the idField above empty (it will generate a guid for id in Cosmos DB document) or false if you have mapped the id field above.

                useGuidForId=false

        * Set the following boolean variable to true if you have left the pkField above empty (it will generate a guid for pk column in Cosmos DB document) or false if you have mapped the pk field above.

                useGuidForPk=false

        * Provide partition key defined at the time of collection creation in the following field

                CosmosDbDataCollectionPkValue=<PartitionKey>
    * If you're importing data from blob storage provide storage account connection string under the Azure Blob Settings section else if it's data lake provide the connection settings under the Azure Data lake Setting section
    * Provide the Cosmos DB account credentials under the CosmosDB Settings section


## Running the Tool:

Step 1: Run the following command to queue the data files to distribute load across multiple workers and to track the status of each file import:
    
```
java -Xmx8G -jar jsonstore-cosmosdb-import-1.0-SNAPSHOT-jar-with-dependencies.jar -conf <your settings.properties file absolute path> -storeType <adl|azureblob> -queue <your azure data lake store folder which contains all the data files/ azure blob container>

For ADL: 
java -Xmx8G -jar jsonstore-cosmosdb-import-1.0-SNAPSHOT-jar-with-dependencies.jar -conf settings.properties -storeType adl -queue <QueueName>

For Azure Blogs: 
java -Xmx8G -jar jsonstore-cosmosdb-import-1.0-SNAPSHOT-jar-with-dependencies.jar -conf settings.properties -storeType azureblob -queue <ContainerName>
```

Please use azure portal or https://github.com/mingaliu/DocumentDBStudio/releases/tag/0.72 to verify the queue.

Step 2: Execute the following command in Linux VM to start import of data into Cosmos DB data collection
```
nohup java -Xmx8G -jar jsonstore-cosmosdb-import-1.0-SNAPSHOT-jar-with-dependencies.jar -conf <your settings.properties file absolute path> -ingestionFrom cosmosdb >run.out 2>&1 &

Example: nohup java -Xmx8G -jar jsonstore-cosmosdb-import-1.0-SNAPSHOT-jar-with-dependencies.jar -conf settings.properties -ingestionFrom cosmosdb >run.out 2>&1 &
```

> [!NOTE]
> Please run the same command on other VMs if you plan to distribute work across multiple workers.

If you want to test the data import, we support the following input stores:

```
Upload Adl Json docs file 

java -Xmx8G -jar jsonstore-cosmosdb-import-1.0-SNAPSHOT-jar-with-dependencies.jar -conf settings.properties -ingestionFrom adl -ingestionFilePath /test/part-v000-o000-r-00000

Upload Azure Blob file

java -Xmx8G -jar jsonstore-cosmosdb-import-1.0-SNAPSHOT-jar-with-dependencies.jar -conf settings.properties -ingestionFrom azureblob -ingestionFilePath {container name}|{blob full name}

Upload local Json docs file

java -Xmx8G -jar jsonstore-cosmosdb-import-1.0-SNAPSHOT-jar-with-dependencies.jar -conf settings.properties -ingestionFrom local -ingestionFilePath C:\Adobe\part-v000-o000-r-00000.txt
```

Step 3: Check the progress of Migration

Please use azure portal or https://github.com/mingaliu/DocumentDBStudio/releases/tag/0.72 to verify the migration progress using import tracking collection.

## Recommendations

 TBD

## Known issues

 TBD

