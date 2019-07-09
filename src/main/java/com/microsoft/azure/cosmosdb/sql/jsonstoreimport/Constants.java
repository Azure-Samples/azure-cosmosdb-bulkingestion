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

public class Constants {
  public static final String ADL = "adl";
  public static final String LOCAL = "local";
  public static final String AZURE_BLOB = "azureblob";
  public static final String COSMOS_DB = "cosmosdb";
  public static final String SCALE_TEST = "scaletest";
  public static final String CONTAINER_BLOB_SEPERATOR = "|";

  public static final String OPERATION_UNPARTITIONED_WRITES = "unpartitioned-writes";
  public static final String OPERATION_PARTITIONED_WRITES = "partitioned-writes";
  public static final String OPERATION_PARTITION_FILE_WRITE = "partition-file-write";

  public static final String CREATE_USQL_SCRIPTS = "create";
  public static final String SUBMIT_USQL_SCRIPTS = "submit";

}
