/**
 * Copyright (C) 2018-2020 Expedia, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.hotels.hcommon.hive.metastore.paths;

import static com.hotels.hcommon.hive.metastore.util.LocationUtils.locationAsPath;

import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.thrift.TException;

import com.hotels.hcommon.hive.metastore.iterator.PartitionIterator;

class PartitionedTablePathResolver implements TablePathResolver {

  private final Path tableBaseLocation;
  private final Path globPath;
  private final IMetaStoreClient metaStore;
  private final Table table;

  PartitionedTablePathResolver(IMetaStoreClient metaStore, Table table)
      throws NoSuchObjectException, MetaException, TException {
    this.metaStore = metaStore;
    this.table = table;
    tableBaseLocation = locationAsPath(table);
    List<Partition> onePartition = metaStore.listPartitions(table.getDbName(), table.getTableName(), (short) 1);
    if (onePartition.isEmpty()) {
      throw new RuntimeException(String.format("Table %s at location '%s' has no partitions.",
          Warehouse.getQualifiedName(table), tableBaseLocation));
    }
    Path partitionLocation = locationAsPath(onePartition.get(0));
    int branches = partitionLocation.depth() - tableBaseLocation.depth();
    String globSuffix = StringUtils.repeat("*", "/", branches);
    globPath = new Path(tableBaseLocation, globSuffix);
  }

  @Override
  public Path getGlobPath() {
    return globPath;
  }

  @Override
  public Path getTableBaseLocation() {
    return tableBaseLocation;
  }

  @Override
  public Set<Path> getMetastorePaths(short batchSize) throws NoSuchObjectException, MetaException, TException {
    Set<Path> metaStorePaths = new HashSet<>();
    PartitionIterator partitionIterator = new PartitionIterator(metaStore, table, batchSize);
    while (partitionIterator.hasNext()) {
      Partition partition = partitionIterator.next();
      Path location = PathUtils.normalise(locationAsPath(partition));
      if (!location
          .toString()
          .toLowerCase(Locale.ROOT)
          .startsWith(tableBaseLocation.toString().toLowerCase(Locale.ROOT))) {
        throw new RuntimeException(String.format("Check your configuration: '%s' does not appear to be part of '%s'.",
            location, tableBaseLocation));
      }
      metaStorePaths.add(location);
    }
    return metaStorePaths;
  }
}
