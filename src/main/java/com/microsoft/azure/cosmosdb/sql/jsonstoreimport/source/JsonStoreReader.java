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

import com.microsoft.azure.cosmosdb.sql.jsonstoreimport.sdkextensions.AdlStoreClientExtension;
import com.microsoft.azure.cosmosdb.sql.jsonstoreimport.sdkextensions.AzureBlobClientExtension;
import java.util.List;

public class JsonStoreReader {

  private JsonStoreEntity jsonStoreEntity;

  public JsonStoreReader(JsonStoreEntity jsonStoreEntity) {
    this.jsonStoreEntity = jsonStoreEntity;
  }

  public List<String> getJsonDocs() throws Exception {
    switch (this.jsonStoreEntity.storeType) {
      case AZURE_BLOB:
        return AzureBlobClientExtension.getJsonDocs(jsonStoreEntity.location);
      case ADL:
        return AdlStoreClientExtension.getJsonDocs(jsonStoreEntity.location);
      case WINDOWS_FILE_SYSTEM:
        return FileReader.getJsonDocs(jsonStoreEntity.location);
      case TEST:
        return ScaleTestReader.getJsonTestData();
      case PARTITION_TEST:
        return ScaleTestReader.getJsonPartitionTestData();
      default:
        throw new Exception(
            "Missing store type, please try with Adl, WINDOWS_FILE_SYSTEM options.");
    }
  }
}
