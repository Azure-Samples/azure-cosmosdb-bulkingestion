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

import com.microsoft.azure.cosmosdb.sql.jsonstoreimport.source.JsonStoreReader;
import java.util.Stack;

class ReadDocumentGroupRunnable implements Runnable {

  private Stack<DocumentGroup> stackDocs;
  private ImportWorkItem jsonStoreEntity;

  public ReadDocumentGroupRunnable(Stack<DocumentGroup> stackDocs, ImportWorkItem
      jsonStoreEntity) {
    this.stackDocs = stackDocs;
    this.jsonStoreEntity = jsonStoreEntity;
  }

  @Override
  public void run() {
    try {
      if (jsonStoreEntity == null) return;
      JsonStoreReader jsonStoreReader =
          new JsonStoreReader(jsonStoreEntity.getJsonStoreEntityInstance());
      stackDocs.add(new DocumentGroup(jsonStoreEntity, jsonStoreReader.getJsonDocs()));
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}