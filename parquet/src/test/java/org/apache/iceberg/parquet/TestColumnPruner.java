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

package org.apache.iceberg.parquet;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Set;
import org.apache.avro.generic.GenericData;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.FileRewriteStatus;
import org.apache.iceberg.Schema;
import org.apache.iceberg.avro.RandomAvroData;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.Pair;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static org.apache.iceberg.Files.localInput;
import static org.apache.iceberg.Files.localOutput;
import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;

public class TestColumnPruner {

  @Rule
  public TemporaryFolder temp = new TemporaryFolder();

  private static File file;

  private static final Schema fileSchema = new Schema(
      required(1, "id", Types.LongType.get()),
      required(2, "struct", Types.StructType.of(
          required(3, "col1", Types.StringType.get()),
          required(4, "col2", Types.StringType.get())
      ))
  );

  @Before
  public void writeFile() throws IOException {
    int numRecords = 5;
    List<GenericData.Record> records = RandomAvroData.generate(fileSchema, numRecords, 3);
    file = ParquetWritingTestUtils.writeRecords(temp, fileSchema, records.toArray(new GenericData.Record[numRecords]));
  }

  @Test
  public void testNotSkipUnsupportedCols() {
    Schema currentSchema = new Schema(
        required(1, "id", Types.LongType.get()),
        required(2, "struct", Types.StructType.of(
            optional(5, "col3", Types.StringType.get()),
            optional(6, "col4", Types.StringType.get())
        ))
    );

    Assert.assertThrows("Cannot prune columns due compatibility issue",
        UnsupportedOperationException.class,
        () -> ColumnPruner.selectRewriteSchema(currentSchema, localInput(file), null, false));
  }

  @Test
  public void testSkipUnsupportedCols() throws IOException {
    Schema currentSchema = new Schema(
        required(1, "id", Types.LongType.get()),
        required(2, "struct", Types.StructType.of(
            optional(5, "col3", Types.StringType.get()),
            optional(6, "col4", Types.StringType.get())
        ))
    );

    Assert.assertEquals(
        "Should skip prune unsupported columns.",
        FileRewriteStatus.RETAIN, ColumnPruner.selectRewriteSchema(currentSchema, localInput(file), null, true).first());
  }

  @Test
  public void testPrune() throws IOException {
    Schema currentSchema = new Schema(
        required(2, "struct", Types.StructType.of(
            required(3, "col1", Types.StringType.get()),
            optional(5, "col3", Types.StringType.get()),
            optional(6, "col4", Types.StringType.get())
        ))
    );

    Pair<FileRewriteStatus, OutputFile> result = prune(currentSchema, file, true);
    Assert.assertEquals("Should rewrite the file.", FileRewriteStatus.REWRITE, result.first());
    OutputFile outputFile = result.second();
    Set<Integer> expected = Sets.newHashSet(2, 3);

    try (ParquetFileReader reader = ParquetFileReader.open(ParquetIO.file(outputFile.toInputFile()))) {
      ParquetMetadata footer = reader.getFooter();
      Set<Integer> leftIds = extractIds(ParquetUtil.getParquetTypeWithIds(footer, null));
      Assert.assertEquals(expected, leftIds);
    } catch (IOException e) {
      throw new UncheckedIOException(String.format("Failed to open file %s", outputFile.location()), e);
    }
  }

  private Pair<FileRewriteStatus, OutputFile> prune(Schema currentSchema, File file, boolean skipUnsupportedCols) throws IOException {
    Configuration conf = new Configuration();
    File output = temp.newFile();
    Pair<FileRewriteStatus, MessageType> selectResult =
        ColumnPruner.selectRewriteSchema(currentSchema, localInput(file), null, false);

    ColumnPruner.pruneRewrite(conf, localInput(file), localOutput(output), selectResult.second());

    return Pair.of(selectResult.first(), localOutput(output));
  }

  private Set<Integer> extractIds(GroupType typeWithIds) {
    Set<Integer> ids = Sets.newHashSet();
    if (!(typeWithIds instanceof MessageType)) {
      ids.add(typeWithIds.getId().intValue());
    }

    List<Type> fields = typeWithIds.getFields();
    for (Type field : fields) {
      if (field instanceof GroupType) {
        ids.addAll(extractIds(field.asGroupType()));
      } else {
        ids.add(field.getId().intValue());
      }
    }

    return ids;
  }
}
