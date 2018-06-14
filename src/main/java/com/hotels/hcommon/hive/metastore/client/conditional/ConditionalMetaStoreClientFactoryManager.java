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

import java.util.List;

import com.hotels.hcommon.hive.metastore.client.api.ConditionalMetaStoreClientFactory;
import com.hotels.hcommon.hive.metastore.client.api.MetaStoreClientFactory;

public class ConditionalMetaStoreClientFactoryManager {
  private final List<ConditionalMetaStoreClientFactory> metaStoreClientFactories;

  public ConditionalMetaStoreClientFactoryManager(List<ConditionalMetaStoreClientFactory> metaStoreClientFactories) {
    this.metaStoreClientFactories = metaStoreClientFactories;
  }

  public MetaStoreClientFactory factoryForUri(String uri) {
    for (ConditionalMetaStoreClientFactory metaStoreClientFactory : metaStoreClientFactories) {
      if (metaStoreClientFactory.accepts(uri)) {
        return metaStoreClientFactory;
      }
    }
    throw new IllegalArgumentException("No MetaStoreClientFactory found for uri " + uri);
  }
}
