User Guide for importing Sorted data by Partition Id

Prerequisites:

1. Provision Azure data lake account and Azure data analytics account. Please make sure 'Azure data analytics account' has high degree of parallelism for large volumes of data.

2. Load the source data into Azure data lake account.

3. Enable [Authentication using active directory](https://docs.microsoft.com/en-us/azure/data-lake-store/data-lake-store-service-to-service-authenticate-using-active-directory) for analytics account, note down Tenant Id, Client Id and Client Secret key.

Step 1: Setup azure analytics runtime using preview bits. please reach out to cosmosdbninjas@microsoft.com to get these bits and register as described in this [blog](https://blogs.msdn.microsoft.com/azuredatalake/2016/08/26/how-to-register-u-sql-assemblies-in-your-u-sql-catalog/)

Step 2: Update analytics account and other additional for creating and submitting Usql jobs :

```
//Azure analytics account
adlaAccountName=adobeusql
adlaTenantId=enter-adlaTenantId
adlaSubId=enter-adlaSubId
adlaClientId=enter-adlaClientId
adlaClientSecret=enter-adlaClientSecret
shuffleUsqlScriptOutputFolder=shuffle
sortUsqlScriptOutputFolder=sort

//Json payload column in CSV
//For debugging purpose script emits rownumber and hashkey along with Json Doc
//jsonDocColumnIndexInCsv=1

//By default USql allows to submit only 200 jobs
numberOfUsqlJobs=200

//Applies only to shuffle
//All files source folders sorted by name in ascending order
//and chooses number of files to be processed
numberOfSourceDataFilesToProcess=30000

numberOfCosmosDbPartitions=10

//Keep big number so it will order the files based on the max limit
estimatedNumberOfFilesInPartition=100

//Azure Data Lake Settings
adlAccountFQDN=enter-adlAccountFQDN
adlClientId=enter-adlaClientSecret
adlAuthTokenEndpoint=enter-adlAuthTokenEndpoint
adlSourceFolder=/backcompattest/
adlShuffleFolder=/shuffleTest/
adlSortedDataFolder=/sortedDummyData/
```
Step 3: Shuffling data

1. Use json-import tool to generate Usql scripts which partitions the data by partition id based on the size of the collection. Following is the command generates two folders Shuffle and Sort.

   ``` nohup java -Xmx8G -jar jsonstore-cosmosdb-import-1.0-SNAPSHOT-jar-with-dependencies.jar -conf {your settings.properties file absolute path} -usql create >run.out 2>&1 & ``` 

2. Submit the jobs using the following command:

   `nohup java -Xmx8G -jar jsonstore-cosmosdb-import-1.0-SNAPSHOT-jar-with-dependencies.jar -conf {your settings.properties file absolute path} -usql submit -sh >run.out 2>&1 &`

Step 4: Sort the data

`nohup java -Xmx8G -jar jsonstore-cosmosdb-import-1.0-SNAPSHOT-jar-with-dependencies.jar -conf {your settings.properties file absolute path} -usql submit -st >run.out 2>&1 &`

Step 5: Ingest data to Cosmos DB

please set the following settings based on the infrastructure which controls how many partitions can be processed from each ingestion worker and number of documents it can read from a file

// Ingestion worker settings
partitionsLimitForWorker=135
jsonDocsBatchCount=20000

`nohup java -Xmx8G -jar jsonstore-cosmosdb-import-1.0-SNAPSHOT-jar-with-dependencies.jar -conf {your settings.properties file absolute path} -ingestionFrom cosmosdb -s  >run.out`