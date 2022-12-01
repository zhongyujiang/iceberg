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

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.FileRewriteStatus;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.mapping.NameMapping;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.Pair;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ColumnPruner {
  private static final Logger LOG = LoggerFactory.getLogger(ColumnPruner.class);

  private ColumnPruner() {
  }

  public static Pair<FileRewriteStatus, MessageType> selectRewriteSchema(Schema currentSchema,
      InputFile inputFile, NameMapping nameMapping, boolean skipUnsupportedCols) {
    try (ParquetFileReader reader = ParquetFileReader.open(ParquetIO.file(inputFile))) {
      ParquetMetadata pmd = reader.getFooter();
      MessageType typeWithIds = ParquetUtil.getParquetTypeWithIds(pmd, nameMapping);
      Map<Integer, Types.NestedField> idToField = TypeUtil.indexById(currentSchema.asStruct());
      Set<Integer> tableIds = idToField.keySet();
      Set<Integer> fileIds = extractIds(typeWithIds);

      Sets.SetView<Integer> droppedIds = Sets.difference(fileIds, tableIds);
      if (droppedIds.size() == fileIds.size()) {
        // all columns are needed to be pruned
        return Pair.of(FileRewriteStatus.REMOVE, null);
      }

      if (droppedIds.isEmpty()) {
        // no columns are needed to be pruned
        return Pair.of(FileRewriteStatus.RETAIN, null);
      }

      Set<Integer> shouldPruneIds = Sets.newHashSet(droppedIds);
      shouldPruneIds(typeWithIds, shouldPruneIds);
      Set<Integer> addedIds = Sets.newHashSet(Sets.difference(shouldPruneIds, droppedIds));

      for (Integer addedId : addedIds) {
        if (idToField.get(addedId).isRequired()) {
          Type type = findType(typeWithIds, addedId);
          Set<Integer> typeIds = extractIds(type.asGroupType());
          if (skipUnsupportedCols) {
            // TODO: Only leave the smallest size column to maintain compatibility.
            LOG.warn("Cannot prune column {} due to compatibility issue, will ignore this column.", type.getName());
            shouldPruneIds.removeAll(typeIds);
          } else {
            throw new UnsupportedOperationException(
                String.format("Cannot prune column %s: %s in file %s due to compatibility issue.",
                    type.getId(), type.getName(), inputFile.location()));
          }
        }
      }

      if (shouldPruneIds.isEmpty()) {
        // no columns can be pruned
        return Pair.of(FileRewriteStatus.RETAIN, null);
      }

      Sets.SetView<Integer> leftIds = Sets.difference(fileIds, shouldPruneIds);
      MessageType leftSchema = (MessageType) ParquetTypeVisitor.visit(typeWithIds, new PruneColumns(leftIds));
      return Pair.of(FileRewriteStatus.REWRITE, leftSchema);
    } catch (IOException e) {
      throw new UncheckedIOException(String.format("Failed to open file: %s", inputFile.location()), e);
    }
  }

  public static void pruneRewrite(Configuration conf, InputFile inputFile, OutputFile outputFile,
      MessageType rewriteSchema) {
    try (ParquetFileReader reader = ParquetFileReader.open(ParquetIO.file(inputFile))) {
      Map<String, String> keyValueMetaData = reader.getFileMetaData().getKeyValueMetaData();
      Map<String, String> newKeyValueMetaData = Maps.newHashMap(keyValueMetaData);
      ParquetFileWriter writer;
      try {
         writer = new ParquetFileWriter(
            ParquetIO.file(outputFile, conf), rewriteSchema, ParquetFileWriter.Mode.OVERWRITE, 0, 0);
      } catch (IOException e) {
        throw new UncheckedIOException("Failed to create Parquet file", e);
      }

      try {
        writer.start();
        writer.appendFile(ParquetIO.file(inputFile));
        Schema schema = ParquetSchemaUtil.convert(rewriteSchema);
        newKeyValueMetaData.put("iceberg.schema", SchemaParser.toJson(schema));
        writer.end(newKeyValueMetaData);
      } catch (IOException e) {
        throw new UncheckedIOException("Failed to start Parquet file writer", e);
      }

    } catch (IOException e) {
      throw new UncheckedIOException(String.format("Failed to open file: %s", inputFile.location()), e);
    }
  }

  // Add the parent id if all direct child fields are all dropped.
  private static void shouldPruneIds(GroupType typeWithIds, Set<Integer> shouldPruneIds) {
    List<Type> fields = typeWithIds.getFields();
    for (Type field : fields) {
      if (field instanceof GroupType && !shouldPruneIds.contains(field.getId().intValue())) {
        shouldPruneIds(field.asGroupType(), shouldPruneIds);
        List<Integer> childIds = ((GroupType) field).getFields().stream().map(child -> child.getId().intValue()).collect(Collectors.toList());
        if (shouldPruneIds.containsAll(childIds)) {
          shouldPruneIds.add(field.getId().intValue());
        }
      }
    }
  }

  private static Set<Integer> extractIds(GroupType typeWithIds) {
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

  private static Type findType(GroupType typeWithIds, int id) {
    List<Type> fields = typeWithIds.getFields();
    for (Type field : fields) {
      if (field.getId().intValue() == id) {
        return field;
      }
      if (field instanceof GroupType) {
        return findType(field.asGroupType(), id);
      }
    }

    return null;
  }
}

