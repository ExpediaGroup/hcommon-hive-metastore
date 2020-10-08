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

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.when;

import java.net.URISyntaxException;
import java.util.Collections;

import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.thrift.TException;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class TablePathResolverTest {

  private static final String DATABASE_NAME = "database";
  private static final String TABLE_NAME = "table";

  private @Mock Table table;
  private @Mock Partition partition;
  private @Mock StorageDescriptor tableSd;
  private @Mock StorageDescriptor partitionSd;
  private @Mock IMetaStoreClient metaStoreClient;

  @Before
  public void setUp() {
    when(table.getSd()).thenReturn(tableSd);
    when(table.getDbName()).thenReturn(DATABASE_NAME);
    when(table.getTableName()).thenReturn(TABLE_NAME);
  }

  @Test
  public void partitioned() throws TException, URISyntaxException {
    when(table.getPartitionKeys()).thenReturn(Collections.singletonList(new FieldSchema()));
    when(tableSd.getLocation()).thenReturn("s3://bucket/db/table");
    when(metaStoreClient.listPartitions(DATABASE_NAME, TABLE_NAME, (short) 1)).thenReturn(
        Collections.singletonList(partition));
    when(partition.getSd()).thenReturn(partitionSd);
    when(partitionSd.getLocation()).thenReturn("s3://bucket/db/table/snapshot1/year=2000");
    TablePathResolver resolver = TablePathResolver.Factory.newTablePathResolver(metaStoreClient, table);
    assertThat(resolver, instanceOf(PartitionedTablePathResolver.class));
  }

  @Test
  public void unpartitioned() throws TException, URISyntaxException {
    when(tableSd.getLocation()).thenReturn("s3://bucket/db/table/snapshot1");
    TablePathResolver resolver = TablePathResolver.Factory.newTablePathResolver(metaStoreClient, table);
    assertThat(resolver, instanceOf(UnpartitionedTablePathResolver.class));
  }
}
