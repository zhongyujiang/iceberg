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

package org.apache.iceberg.spark.actions;

import java.util.List;
import java.util.Set;
import java.util.UUID;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.actions.PruneColumnsStrategy;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.LocationProvider;
import org.apache.iceberg.mapping.NameMapping;
import org.apache.iceberg.mapping.NameMappingParser;
import org.apache.iceberg.spark.FileRewriteCoordinator;
import org.apache.iceberg.spark.FileScanTaskSetManager;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.internal.SQLConf;

public class SparkPruneColumnsStrategy extends PruneColumnsStrategy {
  private final Table table;
  private final String fullIdentifier;
  private final SparkSession spark;
  private final FileScanTaskSetManager manager = FileScanTaskSetManager.get();
  private final FileRewriteCoordinator rewriteCoordinator = FileRewriteCoordinator.get();

  public SparkPruneColumnsStrategy(Table table, String fullIdentifier, SparkSession spark) {
    this.table = table;
    this.spark = spark;
    // Fallback if a quoted identifier is not supplied
    this.fullIdentifier = fullIdentifier == null ? table.name() : fullIdentifier;
  }

  @Override
  public Table table() {
    return table;
  }

  @Override
  public Set<DataFile> rewriteFiles(List<FileScanTask> filesToRewrite) {

    String groupID = UUID.randomUUID().toString();
    try {
      manager.stageTasks(table, groupID, filesToRewrite);

      JavaSparkContext sparkContext = JavaSparkContext.fromSparkContext(spark.sparkContext());
      JavaRDD<FileScanTask> filesRDD = sparkContext.parallelize(filesToRewrite);

      LocationProvider locationProvider = table().locationProvider();
      // locationProvider.newDataLocation()
      // locationProvider.newDataLocation(null, null, 12)

      String nameMappingString = table.properties().get(TableProperties.DEFAULT_NAME_MAPPING);
      NameMapping nameMapping = nameMappingString != null ? NameMappingParser.fromJson(nameMappingString) : null;

      FileIO io = table.io();

      // FindColumnsToPrune findColumnPaths = new FindColumnsToPrune(nameMapping, columnIds());
      //
      // filesRDD.map()
      //
      // JavaPairRDD<FileScanTask, List<String>> pairRDD =
      //     filesRDD.mapToPair(new PairFunction<FileScanTask, FileScanTask, List<String>>() {
      //
      //       @Override
      //       public Tuple2<FileScanTask, List<String>> call(FileScanTask fileScanTask) throws Exception {
      //         PartitionSpec spec = fileScanTask.spec();
      //         List<String> paths = findColumnPaths.findColumnPaths(table.io().newInputFile(fileScanTask.file().path().toString()));
      //         return Tuple2.apply(fileScanTask, paths);
      //       }
      //     });
      //
      // ColumnPruner columnPruner = new ColumnPruner();
      // Configuration conf = new Configuration();
      // pairRDD.map(pair -> columnPruner.pruneColumns(conf, pair._1.file()))


      // Disable Adaptive Query Execution as this may change the output partitioning of our write
      SparkSession cloneSession = spark.cloneSession();
      cloneSession.conf().set(SQLConf.ADAPTIVE_EXECUTION_ENABLED().key(), false);


      // cloneSession.sparkContext().executorAllocationManager()

    } finally {

    }


    return null;
  }
}
