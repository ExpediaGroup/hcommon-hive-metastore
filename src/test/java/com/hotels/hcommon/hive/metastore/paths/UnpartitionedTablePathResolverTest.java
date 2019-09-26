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
import java.util.Set;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.thrift.TException;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class UnpartitionedTablePathResolverTest {

  private static final String DATABASE_NAME = "database";
  private static final String TABLE_NAME = "table";
  private TablePathResolver tablePathResolver;
  private @Mock Table table;
  private @Mock StorageDescriptor tableSd;

  @Before
  public void setUp() {
    when(table.getDbName()).thenReturn(DATABASE_NAME);
    when(table.getTableName()).thenReturn(TABLE_NAME);
    when(table.getSd()).thenReturn(tableSd);
    when(tableSd.getLocation()).thenReturn("file:///tmp/data/eventId/");

    tablePathResolver = new UnpartitionedTablePathResolver(table);
  }

  @Test
  public void globPath() {
    Path globPath = tablePathResolver.getGlobPath();
    String expected = new Path("file:/tmp/data/*").toString();
    assertThat(globPath.toString(), is(expected));
  }

  @Test
  public void tableBaseLocation() {
    Path tableBaseLocation = tablePathResolver.getTableBaseLocation();
    String expected = new Path("file:/tmp/data").toString();
    assertThat(tableBaseLocation.toString(), is(expected));
  }

  @Test
  public void metaStorePaths() throws TException, URISyntaxException {
    Set<Path> metaStorePaths = tablePathResolver.getMetastorePaths((short) 1000);
    Path expected = new Path("file:/tmp/data/eventId");
    assertThat(metaStorePaths.size(), is(1));
    assertThat(metaStorePaths.iterator().next().toString(), is(expected.toString()));
  }
}
