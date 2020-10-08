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
package com.hotels.hcommon.hive.metastore.client.supplier;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;

import org.apache.hadoop.hive.conf.HiveConf;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import com.hotels.hcommon.hive.metastore.client.api.MetaStoreClientFactory;

@RunWith(MockitoJUnitRunner.class)
public class HiveMetaStoreClientSupplierTest {

  private @Mock MetaStoreClientFactory metaStoreClientFactory;
  private @Mock HiveConf hiveConf;
  private String name = "name";

  @Test
  public void get() {
    HiveMetaStoreClientSupplier metaStoreClientSupplier = new HiveMetaStoreClientSupplier(metaStoreClientFactory, hiveConf, name);
    metaStoreClientSupplier.get();
    verify(metaStoreClientFactory).newInstance(eq(hiveConf), eq(name));
  }
}
