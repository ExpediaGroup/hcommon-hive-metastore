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

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.when;

import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Set;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.thrift.TException;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class PartitionedTablePathResolverTest {

  private static final String PARTITION_NAME_1 = "partition=1/x=y";
  private static final String PARTITION_NAME_2 = "partition=2/x=z";
  private static final String TABLE_LOCATION = "file:///tmp/data/";
  private static final String PARTITION_LOCATION_1 = "file:///tmp/data/eventId/year/month/01";
  private static final String PARTITION_LOCATION_2 = "file:///tmp/data/eventId/year/month/02";
  private static final String DATABASE_NAME = "database";
  private static final String TABLE_NAME = "table";

  private @Mock Table table;
  private @Mock StorageDescriptor tableSd;
  private @Mock Partition partition1;
  private @Mock Partition partition2;
  private @Mock StorageDescriptor partition1Sd;
  private @Mock StorageDescriptor partition2Sd;
  private @Mock IMetaStoreClient metaStore;

  private TablePathResolver resolver;

  @Before
  public void setUp() {
    when(table.getDbName()).thenReturn(DATABASE_NAME);
    when(table.getTableName()).thenReturn(TABLE_NAME);
    when(table.getSd()).thenReturn(tableSd);
    when(tableSd.getLocation()).thenReturn(TABLE_LOCATION);
  }

  @Test(expected = RuntimeException.class)
  public void metaStorePathsNoPartitions() throws TException {
    when(metaStore.listPartitions(DATABASE_NAME, TABLE_NAME, (short) 1)).thenReturn(Collections.<Partition>emptyList());
    new PartitionedTablePathResolver(metaStore, table);
  }

  @Test
  public void metaStorePaths() throws TException, URISyntaxException {
    setUpPartitionedResolver();
    setUpMetaStore();

    Set<Path> metaStorePaths = resolver.getMetastorePaths((short) 100);
    assertThat(metaStorePaths.size(), is(2));
    assertThat(metaStorePaths.contains(new Path(PARTITION_LOCATION_1)), is(true));
    assertThat(metaStorePaths.contains(new Path(PARTITION_LOCATION_2)), is(true));
  }

  @Test
  public void globPath() throws TException {
    setUpPartitionedResolver();
    Path globPath = resolver.getGlobPath();
    String expected = "file:/tmp/data/*/*/*/*";
    assertThat(globPath.toString(), is(expected));
  }

  @Test
  public void tableBaseLocation() throws TException {
    setUpPartitionedResolver();
    Path tableBaseLocation = resolver.getTableBaseLocation();
    String expected = "file:/tmp/data";
    assertThat(tableBaseLocation.toString(), is(expected));
  }

  private void setUpPartitionedResolver() throws TException {
    when(partition1.getSd()).thenReturn(partition1Sd);
    when(partition1Sd.getLocation()).thenReturn(PARTITION_LOCATION_1);
    when(metaStore.listPartitions(DATABASE_NAME, TABLE_NAME, (short) 1)).thenReturn(
        Collections.singletonList(partition1));
    resolver = new PartitionedTablePathResolver(metaStore, table);
  }

  private void setUpMetaStore() throws TException {
    when(metaStore.getPartitionsByNames(DATABASE_NAME, TABLE_NAME,
        Arrays.asList(PARTITION_NAME_1, PARTITION_NAME_2))).thenReturn(Arrays.asList(partition1, partition2));
    when(metaStore.listPartitionNames(DATABASE_NAME, TABLE_NAME, (short) -1)).thenReturn(
        Arrays.asList(PARTITION_NAME_1, PARTITION_NAME_2));

    when(partition2.getSd()).thenReturn(partition2Sd);
    when(partition2Sd.getLocation()).thenReturn(PARTITION_LOCATION_2);
  }
}
