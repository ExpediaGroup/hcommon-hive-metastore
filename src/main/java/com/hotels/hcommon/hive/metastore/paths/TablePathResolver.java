/**
 * Copyright (C) 2018-2019 Expedia Inc.
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

import java.net.URISyntaxException;
import java.util.Set;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public interface TablePathResolver {

  Path getGlobPath();

  Path getTableBaseLocation();

  Set<Path> getMetastorePaths(short batchSize)
      throws NoSuchObjectException, MetaException, TException, URISyntaxException;

  class Factory {

    private static final Logger log = LoggerFactory.getLogger(TablePathResolver.Factory.class);

    static TablePathResolver newTablePathResolver(
        IMetaStoreClient metastore, Table table)
        throws URISyntaxException, NoSuchObjectException, MetaException, TException {
      if (!table.getPartitionKeys().isEmpty()) {
        log.debug("Table '{}' is partitioned", Warehouse.getQualifiedName(table));
        return new PartitionedTablePathResolver(metastore, table);
      } else {
        log.debug("Table '{}' is unpartitioned", Warehouse.getQualifiedName(table));
        return new UnpartitionedTablePathResolver(table);
      }
    }
  }
}
