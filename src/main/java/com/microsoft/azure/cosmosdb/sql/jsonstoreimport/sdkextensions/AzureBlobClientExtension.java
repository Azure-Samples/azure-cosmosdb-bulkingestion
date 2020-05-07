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

package com.microsoft.azure.cosmosdb.sql.jsonstoreimport.sdkextensions;

import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.blob.models.BlobItem;
import com.microsoft.azure.cosmosdb.sql.jsonstoreimport.Constants;
import com.microsoft.azure.cosmosdb.sql.jsonstoreimport.Settings;
import com.microsoft.azure.cosmosdb.sql.jsonstoreimport.source.FileReader;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URISyntaxException;
import java.security.InvalidKeyException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;
import org.apache.log4j.Logger;

public class AzureBlobClientExtension {

  private static final Logger logger = Logger.getLogger(AzureBlobClientExtension.class);
  private static BlobServiceClient blobServiceClient;
  private static BlobContainerClient blobContainerClient;

  public static List<String> getJsonDocs(String location) throws URISyntaxException, IOException, InvalidKeyException {
    // Location contains container and blob file name
    String[] parts = location.split(Pattern.quote(Constants.CONTAINER_BLOB_SEPERATOR));
    initClient(parts[0]);
    BlobClient blobClient = blobContainerClient.getBlobClient(parts[1]);
    FileOutputStream fileOutputStream = new FileOutputStream(blobClient.getBlobName());
    logger.info("Downloading blob: " + parts[1]);
    blobClient.download(fileOutputStream);
    logger.info("Download completed");
    fileOutputStream.close();
    return FileReader.getJsonDocs(blobClient.getBlobName(), true);
  }

  public static void initClient(String container) throws URISyntaxException, InvalidKeyException {
    if (blobContainerClient != null) {
      return;
    }
    blobServiceClient = new BlobServiceClientBuilder().connectionString(Settings.getAzureBlobConnectionString())
        .buildClient();
    blobContainerClient = blobServiceClient.getBlobContainerClient(container);
  }

  public static List<String> getListOfBlobs(String container) throws InvalidKeyException, URISyntaxException {
    initClient(container);
    List<String> listOfBlobs = new ArrayList<>();
    for (BlobItem blobItem : blobContainerClient.listBlobs()) {
        // Add container name and blob name using separator
        listOfBlobs.add(container + Constants.CONTAINER_BLOB_SEPERATOR + blobItem.getName());
    }
    return listOfBlobs;
  }
}
