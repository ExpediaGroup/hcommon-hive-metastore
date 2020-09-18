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
package com.hotels.hcommon.hive.metastore.client.closeable;

import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.thrift.TApplicationException;
import org.apache.thrift.TException;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import com.hotels.hcommon.hive.metastore.client.api.CloseableMetaStoreClient;
import com.hotels.hcommon.hive.metastore.compatibility.HiveMetaStoreClientCompatibility;

@RunWith(MockitoJUnitRunner.class)
public class Hive12CompatibleMetaStoreClientFactoryTest {

  private @Mock HiveMetaStoreClient delegate;
  private @Mock HiveMetaStoreClientCompatibility compatibility;

  private Hive12CompatibleMetaStoreClientFactory factory = new Hive12CompatibleMetaStoreClientFactory();

  @Test
  public void typical() throws TException {
    try (CloseableMetaStoreClient wrapped = factory.newInstance(delegate)) {
      wrapped.unlock(1L);
    }
    verify(delegate).unlock(1L);
    verify(delegate).close();
  }

  @Test
  public void compatibility() throws TException {
    when(delegate.getTable("db", "tbl")).thenThrow(new TApplicationException());
    try (CloseableMetaStoreClient wrapped = factory.newInstance(delegate, compatibility)) {
      wrapped.getTable("db", "tbl");
    }
    verify(compatibility).getTable("db", "tbl");
  }
}
