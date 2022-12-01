/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.iceberg.data;

import java.util.UUID;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileRewriteStatus;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Schema;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.mapping.NameMapping;
import org.apache.iceberg.parquet.ColumnPruner;
import org.apache.iceberg.relocated.com.google.common.base.Joiner;
import org.apache.iceberg.util.Pair;
import org.apache.parquet.schema.MessageType;

/**
 * Rewrite a single data file.
 */
public class FileRewrite {
  private final Configuration conf;
  private final Schema currentSchema;
  private final FileIO io;
  private final FileScanTask fileScanTask;
  private final NameMapping nameMapping;
  private final boolean skipUnsupportedColumns;
  private FileRewriteStatus fileRewriteStatus;
  private DataFile producedFile;
  private final OutputFileFactory outputFileFactory = null;

  public FileRewrite(Configuration conf, Schema currentSchema, FileIO io, FileScanTask fileScanTask,
      NameMapping nameMapping,
      boolean skipUnsupportedColumns) {
    this.conf = conf;
    this.currentSchema = currentSchema;
    this.io = io;
    this.fileScanTask = fileScanTask;
    this.nameMapping = nameMapping;
    this.skipUnsupportedColumns = skipUnsupportedColumns;
  }

  public DataFile pruneRewrite() {
    switch (fileScanTask.file().format()) {
      case PARQUET:
        return pruneParquetFile();
      default:
        throw new IllegalArgumentException("Unexpected file format: " + fileScanTask.file().format());
    }
  }

  private DataFile pruneParquetFile() {
    Pair<FileRewriteStatus, MessageType> selectResult = ColumnPruner.selectRewriteSchema(
        currentSchema,
        io.newInputFile(fileScanTask.file().path().toString()),
        nameMapping,
        skipUnsupportedColumns
    );

    this.fileRewriteStatus = selectResult.first();
    if (!fileRewriteStatus.equals(FileRewriteStatus.REWRITE)) return null;

    String outputFilePath = generateOutputFilePath();
    OutputFile outputFile = io.newOutputFile(generateOutputFilePath());
    ColumnPruner.pruneRewrite(
        conf,
        io.newInputFile(fileScanTask.file().path().toString()),
        io.newOutputFile(outputFilePath),
        selectResult.second()
        );

    DataFile sourceFile = fileScanTask.file();

    DataFile dataFile = DataFiles.builder(fileScanTask.spec())
        .withFormat(sourceFile.format())
        .withPath(outputFilePath)
        .withPartition(sourceFile.partition())
        .withEncryptionKeyMetadata(sourceFile.keyMetadata())
        .withMetrics(null)
        // .withSplitOffsets(ParquetUtil.getSplitOffsets(writer.getFooter()))
        .build();

    return dataFile;
  }

  public FileRewriteStatus fileRewriteStatus() {
    return fileRewriteStatus;
  }

  // TODO: Fix this.
  // Is there a way that we can get the partition data even if not read data from a file?
  private String generateOutputFilePath() {
    String sourcePath = fileScanTask.file().path().toString();
    String[] splits = sourcePath.split("/");
    splits[splits.length-1] = UUID.randomUUID().toString();
    return Joiner.on("/").join(splits);
  }
}
