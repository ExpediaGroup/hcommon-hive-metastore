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
package com.hotels.hcommon.hive.metastore.client.closeable;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.metastore.HiveMetaHook;
import org.apache.hadoop.hive.metastore.HiveMetaHookLoader;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.RetryingMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hotels.hcommon.hive.metastore.client.api.CloseableMetaStoreClient;
import com.hotels.hcommon.hive.metastore.client.api.MetaStoreClientFactory;
import com.hotels.hcommon.hive.metastore.exception.MetaStoreClientException;

public class CloseableMetaStoreClientFactory implements MetaStoreClientFactory {
  private static final Logger log = LoggerFactory.getLogger(CloseableMetaStoreClientFactory.class);

  private static class DisabledHiveMetaHookLoader implements HiveMetaHookLoader {
    @Override
    public HiveMetaHook getHook(Table tbl) throws MetaException {
      return null;
    }
  }

  private final Hive12CompatibleMetaStoreClientFactory hive12CompatibleClientFactory;

  public CloseableMetaStoreClientFactory() {
    hive12CompatibleClientFactory = new Hive12CompatibleMetaStoreClientFactory();
  }

  @Override
  public CloseableMetaStoreClient newInstance(HiveConf hiveConf, String name) {
    log.info("Connecting to '{}' metastore at '{}'", name, hiveConf.getVar(ConfVars.METASTOREURIS));
    try {
      return hive12CompatibleClientFactory
          .newInstance(RetryingMetaStoreClient
              .getProxy(hiveConf, new DisabledHiveMetaHookLoader(), HiveMetaStoreClient.class.getName()));
    } catch (MetaException | RuntimeException e) {
      String message = String
          .format("Unable to connect to '%s' metastore at '%s'", name, hiveConf.getVar(ConfVars.METASTOREURIS));
      throw new MetaStoreClientException(message, e);
    }
  }
}
