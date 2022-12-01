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

package org.apache.iceberg.actions;

import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.base.Predicate;
import org.apache.iceberg.relocated.com.google.common.base.Predicates;
import org.apache.iceberg.relocated.com.google.common.collect.FluentIterable;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.util.BinPacking;
import org.apache.iceberg.util.PropertyUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// Should this strategy produce a REPLACE operation
/**
 * A rewrite strategy for data files which aims to efficiently remove the dropped columns from history data files.
 * <p>
 * It is designed to take advantage of columnar storage like Parquet that can directly prune columns to save the
 * process of encoding and compression when rewrite files.
 */
public abstract class PruneColumnsStrategy implements RewriteStrategy {

  private static final Logger LOG = LoggerFactory.getLogger(PruneColumnsStrategy.class);

  /**
   * Skip unsupported files:
   * 1. unsupported formats files: avro, orc;
   * 2. data files have un-merged delete files;
   * 3. files have different partition spec from current spec.
   * Throws an {@link UnsupportedOperationException} when met unsupported files if this is set to false.
   * TODO: support rewrite data files with only position deletes
   * TODO: support rewrite data files which are only missing identity partition fields or have more fields compared
   * current spec
   */
  public static final String SKIP_UNSUPPORTED_FILES = "skip-unsupported-files";
  public static final boolean SKIP_UNSUPPORTED_FILES_DEFAULT = false;

  /**
   * When all child fields of a struct field in a file are dropped but the parent struct field is not dropped, we
   * cannot prune the whole struct column from file if it's required.
   * <p>
   * Don't prune unsupported columns even if they are selected.
   * This should be a column level action. When pruning a file, if both supported and unsupported
   * columns are selected, the supported column should still be pruned.
   * Throws an {@link UnsupportedOperationException} when pruning unsupported columns if this is set false.
   */
  public static final String SKIP_UNSUPPORTED_COLUMNS = "skip-unsupported-columns";
  public static final boolean SKIP_UNSUPPORTED_COLUMNS_DEFAULT = false;

  Set<FileFormat> SUPPORTED_FORMATS = ImmutableSet.of(
      FileFormat.PARQUET
  );

  private boolean skipUnsupportedFiles;
  private boolean skipUnsupportedColumns;
  private long maxGroupSize;

  @Override
  public String name() {
    return "PRUNE-COLUMNS";
  }

  public boolean skipUnsupportedColumns() {
    return skipUnsupportedColumns;
  }

  @Override
  public Set<String> validOptions() {
    return ImmutableSet.of(
        SKIP_UNSUPPORTED_FILES,
        SKIP_UNSUPPORTED_COLUMNS
    );
  }

  @Override
  public RewriteStrategy options(Map<String, String> options) {
    skipUnsupportedFiles = PropertyUtil.propertyAsBoolean(options,
        SKIP_UNSUPPORTED_FILES,
        SKIP_UNSUPPORTED_FILES_DEFAULT);

    skipUnsupportedColumns = PropertyUtil.propertyAsBoolean(options,
        SKIP_UNSUPPORTED_COLUMNS,
        SKIP_UNSUPPORTED_COLUMNS_DEFAULT);

    maxGroupSize = PropertyUtil.propertyAsLong(options,
        RewriteDataFiles.MAX_FILE_GROUP_SIZE_BYTES,
        RewriteDataFiles.MAX_FILE_GROUP_SIZE_BYTES_DEFAULT);

    Preconditions.checkArgument(maxGroupSize > 0,
        "Invalid max-file-group-size-bytes: %s (must be positive)", maxGroupSize);
    return this;
  }

  @Override
  public Iterable<FileScanTask> selectFilesToRewrite(Iterable<FileScanTask> dataFiles) {
    int currentSpecId = table().spec().specId();
    Predicate<FileScanTask> formatPredict = scanTask -> SUPPORTED_FORMATS.contains(scanTask.file().format());
    Predicate<FileScanTask> deletesPredict = scanTask -> scanTask.deletes().size() == 0;
    Predicate<FileScanTask> specPredict = scanTask -> scanTask.spec().specId() !=  currentSpecId;
    FluentIterable<FileScanTask> filteredFiles = FluentIterable.from(dataFiles);

    if (skipUnsupportedFiles) {
      LOG.info("Table {} set to skip unsupported files.", table().name());
      filteredFiles = filteredFiles.filter(Predicates.and(formatPredict, deletesPredict, specPredict));
    } else {
      if (!filteredFiles.allMatch(formatPredict)) {
        throw new UnsupportedOperationException("Cannot prune files with unmerged delete files.");
      }

      if (!filteredFiles.allMatch(deletesPredict)) {
        throw new UnsupportedOperationException("Can only prune parquet format data files.");
      }

      if (!filteredFiles.allMatch(deletesPredict)) {
        throw new UnsupportedOperationException("Can only prune data files have same spec with current spec.");
      }
    }

    return filteredFiles;
  }

  @Override
  public Iterable<List<FileScanTask>> planFileGroups(Iterable<FileScanTask> dataFiles) {
    BinPacking.ListPacker<FileScanTask> packer = new BinPacking.ListPacker<>(maxGroupSize, 1, false);
    return packer.pack(dataFiles, FileScanTask::length);
  }
}

