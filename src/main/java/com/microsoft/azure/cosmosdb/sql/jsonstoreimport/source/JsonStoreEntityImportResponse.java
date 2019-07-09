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

import com.microsoft.azure.documentdb.bulkexecutor.BulkImportResponse;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.log4j.Logger;

public class JsonStoreEntityImportResponse {

  private final transient Logger logger = Logger.getLogger(JsonStoreEntityImportResponse.class);
  private transient BulkImportResponse bulkImportResponse;
  private int numberOfDocumentsReceived;
  private int numberOfDocumentsImported;
  private List<String> errorInfo;
  private boolean isError;
  private long totalTimeTakenInSeconds;
  private double totalRequestUnitsConsumed;

  public JsonStoreEntityImportResponse() {}

  public JsonStoreEntityImportResponse(List<String> errors) {
    this.errorInfo = errors;
    this.isError = true;
  }

  public JsonStoreEntityImportResponse(
      BulkImportResponse bulkImportResponse, int numberOfDocumentsReceived) {
    this.bulkImportResponse = bulkImportResponse;
    this.numberOfDocumentsReceived = numberOfDocumentsReceived;
    this.numberOfDocumentsImported = bulkImportResponse.getNumberOfDocumentsImported();
    this.totalTimeTakenInSeconds = bulkImportResponse.getTotalTimeTaken().getSeconds();
    this.totalRequestUnitsConsumed = bulkImportResponse.getTotalRequestUnitsConsumed();
  }

  public static JsonStoreEntityImportResponse aggregate(
      List<JsonStoreEntityImportResponse> jsonStoreEntityImportResponses) {
    int numberOfDocumentsReceived = 0;
    int numberOfDocumentsImported = 0;
    long totalTimeTakenInSeconds = 0;
    double totalRequestUnitsConsumed = 0;
    List<String> errorInfo = new ArrayList<>();
    for (JsonStoreEntityImportResponse response : jsonStoreEntityImportResponses) {
      numberOfDocumentsReceived =
          numberOfDocumentsReceived + response.getNumberOfDocumentsReceived();
      numberOfDocumentsImported =
          numberOfDocumentsImported + response.getNumberOfDocumentsImported();
      totalTimeTakenInSeconds = totalTimeTakenInSeconds + response.getTotalTimeTakenInSeconds();
      totalRequestUnitsConsumed =
          totalRequestUnitsConsumed + response.getTotalRequestUnitsConsumed();
      List<String> errorsFromResponse = response.getErrorInfo();
      if (errorsFromResponse != null) {
        errorInfo.add(String.join("|", errorsFromResponse));
      }
    }
    JsonStoreEntityImportResponse aggregatedResponse = new JsonStoreEntityImportResponse();
    aggregatedResponse.setNumberOfDocumentsReceived(numberOfDocumentsReceived);
    aggregatedResponse.setNumberOfDocumentsImported(numberOfDocumentsImported);
    aggregatedResponse.setTotalTimeTakenInSeconds(totalTimeTakenInSeconds);
    aggregatedResponse.setTotalRequestUnitsConsumed(totalRequestUnitsConsumed);
    aggregatedResponse.setErrorInfo(errorInfo);
    return aggregatedResponse;
  }

  public int getNumberOfDocumentsReceived() {
    return numberOfDocumentsReceived;
  }

  public int getNumberOfDocumentsImported() {
    return numberOfDocumentsImported;
  }

  public long getTotalTimeTakenInSeconds() {
    return totalTimeTakenInSeconds;
  }

  public double getTotalRequestUnitsConsumed() {
    return totalRequestUnitsConsumed;
  }

  public List<String> getErrorInfo() {
    return errorInfo;
  }

  public void setErrorInfo(List<String> errorInfo) {
    this.errorInfo = errorInfo;
  }

  public void setTotalRequestUnitsConsumed(double totalRequestUnitsConsumed) {
    this.totalRequestUnitsConsumed = totalRequestUnitsConsumed;
  }

  public void setTotalTimeTakenInSeconds(long totalTimeTakenInSeconds) {
    this.totalTimeTakenInSeconds = totalTimeTakenInSeconds;
  }

  public void setNumberOfDocumentsImported(int numberOfDocumentsImported) {
    this.numberOfDocumentsImported = numberOfDocumentsImported;
  }

  public void setNumberOfDocumentsReceived(int numberOfDocumentsReceived) {
    this.numberOfDocumentsReceived = numberOfDocumentsReceived;
  }

  public boolean isError() {
    if (numberOfDocumentsReceived > numberOfDocumentsImported) {
      logErrors();
      return true;
    }
    return false;
  }

  public void logErrors() {
    logger.error("failed import all the documents");
    logger.error("number of documents received" + numberOfDocumentsReceived);
    logger.error(
        "number of documents imported" + bulkImportResponse.getNumberOfDocumentsImported());
    this.isError = true;
    errorInfo =
        bulkImportResponse
            .getErrors()
            .stream()
            .map(e -> e.getMessage())
            .collect(Collectors.toList());
  }
}
