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
package com.hotels.hcommon.hive.metastore.client.supplier;

import org.apache.hadoop.hive.conf.HiveConf;

import com.google.common.base.Supplier;

import com.hotels.hcommon.hive.metastore.client.api.CloseableMetaStoreClient;
import com.hotels.hcommon.hive.metastore.client.api.MetaStoreClientFactory;
import com.hotels.hcommon.ssh.TunnelableSupplier;

public class HiveMetaStoreClientSupplier implements TunnelableSupplier<CloseableMetaStoreClient>,
    Supplier<CloseableMetaStoreClient> {

  private final MetaStoreClientFactory metaStoreClientFactory;
  private final HiveConf hiveConf;
  private final String name;

  public HiveMetaStoreClientSupplier(MetaStoreClientFactory metaStoreClientFactory, HiveConf hiveConf, String name) {
    this.metaStoreClientFactory = metaStoreClientFactory;
    this.hiveConf = hiveConf;
    this.name = name;
  }

  @Override
  public CloseableMetaStoreClient get() {
    return metaStoreClientFactory.newInstance(hiveConf, name);
  }
}
