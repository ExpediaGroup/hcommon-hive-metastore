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

import java.lang.reflect.Proxy;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;

import com.hotels.hcommon.hive.metastore.MetaStoreClientException;
import com.hotels.hcommon.hive.metastore.client.api.CloseableMetaStoreClient;
import com.hotels.hcommon.hive.metastore.client.api.MetaStoreClientFactory;
import com.hotels.hcommon.hive.metastore.compatibility.HiveMetaStoreClientCompatibility;
import com.hotels.hcommon.hive.metastore.compatibility.HiveMetaStoreClientCompatibility12x;

public final class CloseableMetaStoreClientFactory implements MetaStoreClientFactory {

  private static final Logger log = LoggerFactory.getLogger(CloseableMetaStoreClientFactory.class);

  private final IMetaStoreClient delegate;

  private HiveMetaStoreClientCompatibility compatibility;

  public CloseableMetaStoreClientFactory(HiveConf conf) {
    this(clientForConf(conf));
  }

  public CloseableMetaStoreClientFactory(IMetaStoreClient delegate) {
    this.delegate = delegate;
  }

  @VisibleForTesting
  CloseableMetaStoreClientFactory(IMetaStoreClient delegate, HiveMetaStoreClientCompatibility compatibility) {
    this.delegate = delegate;
    this.compatibility = compatibility;
  }

  private static HiveMetaStoreClient clientForConf(HiveConf conf) {
    try {
      return new HiveMetaStoreClient(conf);
    } catch (MetaException e) {
      throw new MetaStoreClientException(e.getMessage());
    }
  }

  private void tryLoadCompatibility() {
    if (compatibility == null) {
      try {
        compatibility = new HiveMetaStoreClientCompatibility12x(delegate);
      } catch (Throwable t) {
        log.warn("Unable to initialize compatibility", t);
      }
    }
  }

  @Override
  public CloseableMetaStoreClient newInstance() {
    tryLoadCompatibility();
    ClassLoader classLoader = CloseableMetaStoreClient.class.getClassLoader();
    Class<?>[] interfaces = new Class<?>[] { CloseableMetaStoreClient.class };
    CloseableMetaStoreClientInvocationHandler handler = new CloseableMetaStoreClientInvocationHandler(delegate,
        compatibility);
    return (CloseableMetaStoreClient) Proxy.newProxyInstance(classLoader, interfaces, handler);
  }
}
