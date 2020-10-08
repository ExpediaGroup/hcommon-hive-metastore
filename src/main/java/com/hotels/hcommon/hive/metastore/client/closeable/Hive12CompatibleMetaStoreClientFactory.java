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
package com.hotels.hcommon.hive.metastore.client.closeable;

import java.lang.reflect.Proxy;

import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hotels.hcommon.hive.metastore.client.api.CloseableMetaStoreClient;
import com.hotels.hcommon.hive.metastore.compatibility.HiveMetaStoreClientCompatibility;
import com.hotels.hcommon.hive.metastore.compatibility.HiveMetaStoreClientCompatibility12x;

class Hive12CompatibleMetaStoreClientFactory {

  private static final Logger log = LoggerFactory.getLogger(Hive12CompatibleMetaStoreClientFactory.class);

  CloseableMetaStoreClient newInstance(IMetaStoreClient delegate) {
    HiveMetaStoreClientCompatibility compatibility = null;
    try {
      compatibility = new HiveMetaStoreClientCompatibility12x(delegate);
    } catch (Throwable t) {
      log.warn("Unable to initialize compatibility", t);
    }
    return newInstance(delegate, compatibility);
  }

  CloseableMetaStoreClient newInstance(
      IMetaStoreClient delegate,
      HiveMetaStoreClientCompatibility compatibility) {
    ClassLoader classLoader = CloseableMetaStoreClient.class.getClassLoader();
    Class<?>[] interfaces = new Class<?>[] { CloseableMetaStoreClient.class };
    CloseableMetaStoreClientInvocationHandler handler = new CloseableMetaStoreClientInvocationHandler(delegate,
        compatibility);
    return (CloseableMetaStoreClient) Proxy.newProxyInstance(classLoader, interfaces, handler);
  }
}
