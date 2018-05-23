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
package com.hotels.hcommon.hive.metastore.client.tunnelling;

import org.apache.hadoop.hive.conf.HiveConf;

import com.hotels.hcommon.hive.metastore.client.api.CloseableMetaStoreClient;
import com.hotels.hcommon.hive.metastore.client.api.MetaStoreClientFactory;
import com.hotels.hcommon.ssh.SshSettings;
import com.hotels.hcommon.ssh.TunnelableFactory;

public class TunnellingMetaStoreClientSupplierBuilder {

  private static final int DEFAULT_PORT = 22;
  private static final String DEFAULT_LOCALHOST = "localhost";
  private static final int DEFAULT_SESSION_TIMEOUT = 60000;
  private static final boolean DEFAULT_STRICT_HOST_KEY_CHECK = true;

  private String localHost = DEFAULT_LOCALHOST;
  private int sshPort = DEFAULT_PORT;
  private int timeout = DEFAULT_SESSION_TIMEOUT;
  private boolean strictHostKeyChecking = DEFAULT_STRICT_HOST_KEY_CHECK;
  private String sshRoute;
  private String privateKeys;
  private String knownHosts;

  public TunnellingMetaStoreClientSupplier build(HiveConf hiveConf, MetaStoreClientFactory metaStoreClientFactory) {
    return build(hiveConf, metaStoreClientFactory, SshSettings
        .builder()
        .withRoute(sshRoute)
        .withSshPort(sshPort)
        .withPrivateKeys(privateKeys)
        .withKnownHosts(knownHosts)
        .withSessionTimeout(timeout)
        .withStrictHostKeyChecking(strictHostKeyChecking)
        .build());
  }

  private TunnellingMetaStoreClientSupplier build(HiveConf hiveConf, MetaStoreClientFactory metaStoreClientFactory,
      SshSettings sshSettings) {
    return build(hiveConf, metaStoreClientFactory, new TunnelableFactory<CloseableMetaStoreClient>(sshSettings));
  }

  private TunnellingMetaStoreClientSupplier build(HiveConf hiveConf, MetaStoreClientFactory metaStoreClientFactory,
      TunnelableFactory<CloseableMetaStoreClient> tunnelableFactory) {
    return new TunnellingMetaStoreClientSupplier(hiveConf, localHost, metaStoreClientFactory,
        tunnelableFactory);
  }

  public TunnellingMetaStoreClientSupplierBuilder withRoute(String sshRoute) {
    this.sshRoute = sshRoute;
    return this;
  }

  public TunnellingMetaStoreClientSupplierBuilder withPort(int sshPort) {
    this.sshPort = sshPort;
    return this;
  }

  public TunnellingMetaStoreClientSupplierBuilder withPrivateKeys(String privateKeys) {
    this.privateKeys = privateKeys;
    return this;
  }

  public TunnellingMetaStoreClientSupplierBuilder withKnownHosts(String knownHosts) {
    this.knownHosts = knownHosts;
    return this;
  }

  public TunnellingMetaStoreClientSupplierBuilder withLocalHost(String localHost) {
    this.localHost = localHost;
    return this;
  }

  public TunnellingMetaStoreClientSupplierBuilder withTimeout(int timeout) {
    this.timeout = timeout;
    return this;
  }

  public TunnellingMetaStoreClientSupplierBuilder withStrictHostKeyChecking(String strictHostKeyChecking) {
    this.strictHostKeyChecking = "true".equals(strictHostKeyChecking) || "yes".equals(strictHostKeyChecking);
    return this;
  }
}
