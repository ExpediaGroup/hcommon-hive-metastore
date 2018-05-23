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
package com.hotels.hcommon.hive.metastore.client.conditional;

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

import com.google.common.base.Strings;

import com.hotels.hcommon.hive.metastore.MetaStoreClientException;
import com.hotels.hcommon.hive.metastore.client.api.CloseableMetaStoreClient;
import com.hotels.hcommon.hive.metastore.client.api.ConditionalMetaStoreClientFactory;
import com.hotels.hcommon.hive.metastore.client.closeable.CloseableMetaStoreClientFactory;

public class ConditionalRetryingMetaStoreClientFactory implements ConditionalMetaStoreClientFactory {
  private static final Logger LOG = LoggerFactory.getLogger(ConditionalRetryingMetaStoreClientFactory.class);

  public static final String ACCEPT_PREFIX = "thrift:";

  private final HiveConf hiveConf;
  private final String name;


  public ConditionalRetryingMetaStoreClientFactory(HiveConf hiveConf, String name) {
    this.hiveConf = hiveConf;
    this.name = name;
  }

  @Override
  public CloseableMetaStoreClient newInstance() {
    LOG.debug("Connecting to '{}' metastore at '{}'", name, hiveConf.getVar(ConfVars.METASTOREURIS));
    try {
      return new CloseableMetaStoreClientFactory(RetryingMetaStoreClient.getProxy(hiveConf, new HiveMetaHookLoader() {
        @Override
        public HiveMetaHook getHook(Table tbl) throws MetaException {
          return null;
        }
      }, HiveMetaStoreClient.class.getName())).newInstance();
    } catch (MetaException | RuntimeException e) {
      String message = String.format("Unable to connect to '%s' metastore at '%s'", name,
          hiveConf.getVar(ConfVars.METASTOREURIS));
      throw new MetaStoreClientException(message, e);
    }
  }

  @Override
  public boolean accepts(String url) {
    return Strings.nullToEmpty(url).startsWith(ACCEPT_PREFIX);
  }
}
