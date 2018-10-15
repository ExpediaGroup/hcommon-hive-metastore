/**
 * Copyright (C) 2018 Expedia Inc.
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
package com.hotels.hcommon.hive.metastore.iterator;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collections;

import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.thrift.TException;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.hotels.hcommon.hive.metastore.iterator.PartitionIterator.Ordering;

@RunWith(MockitoJUnitRunner.class)
public class PartitionIteratorTest {

  private static final String TABLE_NAME = "table";
  private static final String DATABASE_NAME = "database";
  @Mock
  private IMetaStoreClient metastore;
  @Mock
  private Table table;

  private final Partition p20120101 = newPartition("2012", "01", "01");
  private final Partition p20120102 = newPartition("2012", "01", "02");
  private final Partition p20120103 = newPartition("2012", "01", "03");
  private final Partition p20120104 = newPartition("2012", "01", "04");
  private final Partition p20131009 = newPartition("2013", "10", "09");
  private final Partition p20131011 = newPartition("2013", "10", "11");
  private final Partition p20140305 = newPartition("2014", "03", "05");
  private final Partition p20140324 = newPartition("2014", "03", "24");
  private final Partition p20140513 = newPartition("2014", "05", "13");
  private final Partition p20160804 = newPartition("2016", "08", "04");

  @Before
  public void initMocks() throws MetaException, TException {
    when(table.getDbName()).thenReturn(DATABASE_NAME);
    when(table.getTableName()).thenReturn(TABLE_NAME);

    when(metastore.listPartitionNames(DATABASE_NAME, TABLE_NAME, (short) -1))
        .thenReturn(Arrays
            .asList("year=2012/month=01/day=01", "year=2012/month=01/day=02", "year=2012/month=01/day=03",
                "year=2012/month=01/day=04", "year=2013/month=10/day=09", "year=2013/month=10/day=11",
                "year=2014/month=03/day=05", "year=2014/month=03/day=24", "year=2014/month=05/day=13",
                "year=2016/month=08/day=04"));
  }

  @Test
  public void batching() throws MetaException, TException {
    when(metastore
        .getPartitionsByNames(DATABASE_NAME, TABLE_NAME,
            Arrays.asList("year=2012/month=01/day=01", "year=2012/month=01/day=02", "year=2012/month=01/day=03")))
                .thenReturn(Arrays.asList(p20120101, p20120102, p20120103));
    when(metastore
        .getPartitionsByNames(DATABASE_NAME, TABLE_NAME,
            Arrays.asList("year=2012/month=01/day=04", "year=2013/month=10/day=09", "year=2013/month=10/day=11")))
                .thenReturn(Arrays.asList(p20120104, p20131009, p20131011));
    when(metastore
        .getPartitionsByNames(DATABASE_NAME, TABLE_NAME,
            Arrays.asList("year=2014/month=03/day=05", "year=2014/month=03/day=24", "year=2014/month=05/day=13")))
                .thenReturn(Arrays.asList(p20140305, p20140324, p20140513));
    when(metastore.getPartitionsByNames(DATABASE_NAME, TABLE_NAME, Arrays.asList("year=2016/month=08/day=04")))
        .thenReturn(Arrays.asList(p20160804));

    PartitionIterator iterator = new PartitionIterator(metastore, table, (short) 3);
    assertThat(iterator.hasNext(), is(true));
    assertThat(iterator.next(), is(p20120101));
    assertThat(iterator.hasNext(), is(true));
    assertThat(iterator.next(), is(p20120102));
    assertThat(iterator.hasNext(), is(true));
    assertThat(iterator.next(), is(p20120103));
    assertThat(iterator.hasNext(), is(true));
    assertThat(iterator.next(), is(p20120104));
    assertThat(iterator.hasNext(), is(true));
    assertThat(iterator.next(), is(p20131009));
    assertThat(iterator.hasNext(), is(true));
    assertThat(iterator.next(), is(p20131011));
    assertThat(iterator.hasNext(), is(true));
    assertThat(iterator.next(), is(p20140305));
    assertThat(iterator.hasNext(), is(true));
    assertThat(iterator.next(), is(p20140324));
    assertThat(iterator.hasNext(), is(true));
    assertThat(iterator.next(), is(p20140513));
    assertThat(iterator.hasNext(), is(true));
    assertThat(iterator.next(), is(p20160804));
    assertThat(iterator.hasNext(), is(false));
  }

  @Test
  public void noBatchSmallerThanBatchSize() throws MetaException, TException {
    when(metastore
        .getPartitionsByNames(DATABASE_NAME, TABLE_NAME,
            Arrays
                .asList("year=2012/month=01/day=01", "year=2012/month=01/day=02", "year=2012/month=01/day=03",
                    "year=2012/month=01/day=04", "year=2013/month=10/day=09", "year=2013/month=10/day=11",
                    "year=2014/month=03/day=05", "year=2014/month=03/day=24", "year=2014/month=05/day=13",
                    "year=2016/month=08/day=04")))
                        .thenReturn(Arrays
                            .asList(p20120101, p20120102, p20120103, p20120104, p20131009, p20131011, p20140305,
                                p20140324, p20140513, p20160804));

    PartitionIterator iterator = new PartitionIterator(metastore, table, (short) 11);
    assertThat(iterator.hasNext(), is(true));
    assertThat(iterator.next(), is(p20120101));
    assertThat(iterator.hasNext(), is(true));
    assertThat(iterator.next(), is(p20120102));
    assertThat(iterator.hasNext(), is(true));
    assertThat(iterator.next(), is(p20120103));
    assertThat(iterator.hasNext(), is(true));
    assertThat(iterator.next(), is(p20120104));
    assertThat(iterator.hasNext(), is(true));
    assertThat(iterator.next(), is(p20131009));
    assertThat(iterator.hasNext(), is(true));
    assertThat(iterator.next(), is(p20131011));
    assertThat(iterator.hasNext(), is(true));
    assertThat(iterator.next(), is(p20140305));
    assertThat(iterator.hasNext(), is(true));
    assertThat(iterator.next(), is(p20140324));
    assertThat(iterator.hasNext(), is(true));
    assertThat(iterator.next(), is(p20140513));
    assertThat(iterator.hasNext(), is(true));
    assertThat(iterator.next(), is(p20160804));
    assertThat(iterator.hasNext(), is(false));
  }

  @Test
  public void noPartitions() throws MetaException, TException {
    when(metastore.listPartitionNames(DATABASE_NAME, TABLE_NAME, (short) -1))
        .thenReturn(Collections.<String> emptyList());

    PartitionIterator iterator = new PartitionIterator(metastore, table, (short) 11);
    assertThat(iterator.hasNext(), is(false));
  }

  @Test
  public void reverseOrderWithBatching() throws MetaException, TException {
    when(metastore
        .getPartitionsByNames(DATABASE_NAME, TABLE_NAME,
            Arrays.asList("year=2016/month=08/day=04", "year=2014/month=05/day=13", "year=2014/month=03/day=24")))
                .thenReturn(Arrays.asList(p20140324, p20140513, p20160804));
    when(metastore
        .getPartitionsByNames(DATABASE_NAME, TABLE_NAME,
            Arrays.asList("year=2014/month=03/day=05", "year=2013/month=10/day=11", "year=2013/month=10/day=09")))
                .thenReturn(Arrays.asList(p20131009, p20131011, p20140305));
    when(metastore
        .getPartitionsByNames(DATABASE_NAME, TABLE_NAME,
            Arrays.asList("year=2012/month=01/day=04", "year=2012/month=01/day=03", "year=2012/month=01/day=02")))
                .thenReturn(Arrays.asList(p20120102, p20120103, p20120104));

    when(metastore.getPartitionsByNames(DATABASE_NAME, TABLE_NAME, Arrays.asList("year=2012/month=01/day=01")))
        .thenReturn(Arrays.asList(p20120101));

    PartitionIterator iterator = new PartitionIterator(metastore, table, (short) 3, Ordering.REVERSE);
    assertThat(iterator.hasNext(), is(true));
    assertThat(iterator.next(), is(p20160804));
    assertThat(iterator.hasNext(), is(true));
    assertThat(iterator.next(), is(p20140513));
    assertThat(iterator.hasNext(), is(true));
    assertThat(iterator.next(), is(p20140324));
    assertThat(iterator.hasNext(), is(true));
    assertThat(iterator.next(), is(p20140305));
    assertThat(iterator.hasNext(), is(true));
    assertThat(iterator.next(), is(p20131011));
    assertThat(iterator.hasNext(), is(true));
    assertThat(iterator.next(), is(p20131009));
    assertThat(iterator.hasNext(), is(true));
    assertThat(iterator.next(), is(p20120104));
    assertThat(iterator.hasNext(), is(true));
    assertThat(iterator.next(), is(p20120103));
    assertThat(iterator.hasNext(), is(true));
    assertThat(iterator.next(), is(p20120102));
    assertThat(iterator.hasNext(), is(true));
    assertThat(iterator.next(), is(p20120101));
    assertThat(iterator.hasNext(), is(false));
  }

  private Partition newPartition(String year, String month, String day) {
    Partition result = new Partition();
    result.setValues(Arrays.asList(year, month, day));
    return result;
  }
}
