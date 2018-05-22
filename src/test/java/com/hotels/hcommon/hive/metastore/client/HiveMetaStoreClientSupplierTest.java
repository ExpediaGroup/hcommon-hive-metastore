/**
 * Copyright (C) 2017-2018 Expedia Inc.
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
package com.hotels.hcommon.hive.metastore.client;

import static org.mockito.Mockito.verify;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.hotels.hcommon.hive.metastore.client.api.MetaStoreClientFactory;
import com.hotels.hcommon.hive.metastore.client.provider.HiveMetaStoreClientSupplier;

@RunWith(MockitoJUnitRunner.class)
public class HiveMetaStoreClientSupplierTest {

  private @Mock MetaStoreClientFactory metaStoreClientFactory;

  @Test
  public void get() {
    HiveMetaStoreClientSupplier metaStoreClientSupplier = new HiveMetaStoreClientSupplier(metaStoreClientFactory);
    metaStoreClientSupplier.get();
    verify(metaStoreClientFactory).newInstance();
  }
}
