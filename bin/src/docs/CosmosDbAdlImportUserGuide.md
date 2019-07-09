### Cosmos DB Tool for importing data from Azure Data Lake Store and Azure Blobs

##### Prerequisites:

1. Please setup Azure Data Lake Store and upload your data in chunks, minimum recommended size 200 Mb. [Learn more about Azure Data Lake Store](https://azure.microsoft.com/en-us/services/data-lake-store/) 

2. Setup AAD application to access Data Lake Store from client. [Learn more about AAD registration](https://docs.microsoft.com/en-us/azure/active-directory/active-directory-app-registration)

3. Setup Azure Storage Account and load your data in single container and make sure file names are unique.

4. Setup 2 Cosmos DB Partitioned Collections.

   a. Partitioned collection with more RUs to ingest actual data.

   b. Standard collection for distributing load across multiple workers and for storing the migration status.

   Note: Only account creation is mandatory and rest you can provide in the settings file, tools generates the collections if they don't exist.

   For help reach out cosmosdbasks@microsoft.com

   ​


##### Tool Setup:

1. Generate Jar using Maven: mvn package

2. Update  settings.properties 

   ​

#####                Mapping Id and Partition key columns of Json doc to Cosmos DB Document

&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;`***Maps 'key' column value in input com.microsoft.azure.cosmosdb.sql.jsonstoreimport.source to Cosmos DB document id column value, empty value ignore this setting***`

 &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;idField=key 

&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;`***Maps 'Key' column value in input com.microsoft.azure.cosmosdb.sql.jsonstoreimport.source to Cosmos DB document partition key column value,, empty value ignore this setting***`

&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;pkField=

&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;`**Overwrites  Cosmos DB document id column value with Random Guid, setting false ignore this value**`

&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;useGuidForId=false

&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;`**Overwrites  Cosmos DB document pk column value with Random Guid, setting false ignore this value**`

&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;useGuidForPk=false

&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;`**Provide partition key defined at the time of collection creation, empty value ignores setting partition key value altogether. This is use full when partition key is id column itself.**`

&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;`CosmosDbDataCollectionPkValue=`

##### Running the Tool:

###### Step 1: Queuing data files to distribute load across multiple workers and to track the status of each file import

Use the following command to queue data files.

&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;`java -Xmx8G -jar jsonstore-cosmosdb-import-1.0-SNAPSHOT-jar-with-dependencies.jar -conf {your settings.properties file absolute path} -storeType {adl/azureblob} -queue /{your azure data lake store folder which contains all the data files/ azure blob container}/`

```
For ADL: example: java -Xmx8G -jar jsonstore-cosmosdb-import-1.0-SNAPSHOT-jar-with-dependencies.jar -conf settings.properties -storeType adl -queue /data/

For Azure Blogs: example: java -Xmx8G -jar jsonstore-cosmosdb-import-1.0-SNAPSHOT-jar-with-dependencies.jar -conf settings.properties -storeType azureblob -queue scaletest
```

please use azure portal or https://github.com/mingaliu/DocumentDBStudio/releases/tag/0.72 to verify the queue.

###### Step 2: Start Data Import into Cosmos DB Data collection

Execute the following command in Linux VM to start import

 &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;`nohup java -Xmx8G -jar jsonstore-cosmosdb-import-1.0-SNAPSHOT-jar-with-dependencies.jar -conf {your settings.properties file absolute path} -ingestionFrom cosmosdb >run.out 2>&1 &`

example: nohup java -Xmx8G -jar jsonstore-cosmosdb-import-1.0-SNAPSHOT-jar-with-dependencies.jar -conf settings.properties -ingestionFrom cosmosdb >run.out 2>&1 &

Note: Please run the same command on other VMs if you plan to distribute work across multiple workers.

For testing purpose, following are supported:

```
upload Adl Json docs file 

java -Xmx8G -jar jsonstore-cosmosdb-import-1.0-SNAPSHOT-jar-with-dependencies.jar -conf settings.properties -ingestionFrom adl -ingestionFilePath /test/part-v000-o000-r-00000

upload Azure Blob file

java -Xmx8G -jar jsonstore-cosmosdb-import-1.0-SNAPSHOT-jar-with-dependencies.jar -conf settings.properties -ingestionFrom azureblob -ingestionFilePath {container name}|{blob full name}

upload local Json docs file

java -Xmx8G -jar jsonstore-cosmosdb-import-1.0-SNAPSHOT-jar-with-dependencies.jar -conf settings.properties -ingestionFrom local -ingestionFilePath C:\Adobe\part-v000-o000-r-00000.txt
```



###### Step 3: Check the progress of Migration

please use azure portal or https://github.com/mingaliu/DocumentDBStudio/releases/tag/0.72 to verify the migration progress using import tracking collection.

##### Recommendations

 TBD

##### Known issues

 TBD

