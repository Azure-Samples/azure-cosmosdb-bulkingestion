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

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.google.common.base.MoreObjects;

public class CmdLineOptions {

  private static JCommander jCommander;

  @Parameter(names = "-ingestionFilePath", description = "Ingestion file path")
  private String ingestionFilePath;

  @Parameter(names = "-conf", description = "Over writes default configuration file")
  private String confFile;

  @Parameter(
    names = "-queue",
    description =
        "Folder which contains files you want to queue to "
            + "Cosmos DB. Example queuing ADL folder files: -queue /data/"
  )
  private String queue;

  @Parameter(
    names = "-ingestionFrom",
    description =
        "Path to a specific a file or Queue name. "
            + "Example for processing files queued in cosmos db: -ingestionFrom cosmosdb"
  )
  private String ingestionFrom;

  @Parameter(names = "-storeType", description = "Specify store type adl or azureblob")
  private String storeType;

  @Parameter(
    names = "-s",
    description = "Use this if data is sorted by partition id using batch " + "processing system"
  )
  private boolean isSorted = false;

  @Parameter(names = "-usql", description = "Creates and submits usql jobs")
  private String usql;

  @Parameter(names = "-sh", description = "Use this to indicate Usql shuffle phase")
  private boolean isShufflePhase = false;

  @Parameter(names = "-st", description = "Use this to indicate Usql sort phase")
  private boolean isSortPhase = false;

  @Parameter(
    names = "-testdata",
    description = "Generates test data based on type of store. " + "Currenlty ADL only supported."
  )
  private String testData;

  static CmdLineOptions init(String[] args) {
    CmdLineOptions cmdLineOptions = new CmdLineOptions();
    jCommander = new JCommander(cmdLineOptions, args);
    return cmdLineOptions;
  }

  static void help() {
    jCommander.usage();
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("ingestionFrom", ingestionFrom)
        .add("ingestionFilePath", ingestionFilePath)
        .add("confFile", confFile)
        .toString();
  }

  boolean getIsShufflePhase() {
    return isShufflePhase;
  }

  boolean getIsSortPhase() {
    return isSortPhase;
  }

  String getIngestionFrom() {
    return ingestionFrom;
  }

  String getIngestionFilePath() {
    return ingestionFilePath;
  }

  String getConfFile() {
    return confFile;
  }

  String getQueue() {
    return queue;
  }

  String getUsql() {
    return usql;
  }

  String getStoreType() {
    return storeType;
  }

  boolean getIsSorted() {
    return isSorted;
  }

  String getTestData() {
    return testData;
  }
}
