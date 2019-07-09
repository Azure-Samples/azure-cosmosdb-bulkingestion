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

package com.microsoft.azure.cosmosdb.sql.jsonstoreimport.sink;

import com.microsoft.azure.cosmosdb.sql.jsonstoreimport.source.JsonStoreEntityImportResponse;
import com.microsoft.azure.documentdb.bulkexecutor.DocumentBulkExecutor;
import java.util.List;
import org.apache.log4j.Logger;

public class CosmosDbSqlWriter {

  private final Logger logger = Logger.getLogger(CosmosDbSqlWriter.class);

  public JsonStoreEntityImportResponse ingestJsonStoreEntity(
      List<String> jsonDocuments, DocumentBulkExecutor bulkImporter) throws Exception {
    logger.info("Initiating CosmosDb import...");
    logger.info("Writing documents to CosmosDb");
    com.microsoft.azure.documentdb.bulkexecutor.BulkImportResponse bulkImportResponse =
        bulkImporter.importAll(jsonDocuments, true, true, null);
    logger.info(
        "Number of documents inserted: " + bulkImportResponse.getNumberOfDocumentsImported());
    logger.info("Import total time: " + bulkImportResponse.getTotalTimeTaken());
    logger.info(
        "Total request unit consumed: " + bulkImportResponse.getTotalRequestUnitsConsumed());
    return new JsonStoreEntityImportResponse(bulkImportResponse, jsonDocuments.size());
  }
}
