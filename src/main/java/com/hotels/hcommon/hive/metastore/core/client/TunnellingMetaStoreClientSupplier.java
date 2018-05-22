/**
 * Copyright (C) 2016-2018 Expedia Inc.
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

package com.hotels.hcommon.hive.metastore.core.client;

import static org.apache.commons.lang3.StringUtils.defaultIfBlank;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;

import com.google.common.base.Supplier;

import com.hotels.hcommon.hive.metastore.core.MetaStoreClientException;
import com.hotels.hcommon.ssh.MethodChecker;
import com.hotels.hcommon.ssh.SshException;
import com.hotels.hcommon.ssh.SshSettings;
import com.hotels.hcommon.ssh.TunnelableFactory;
import com.hotels.hcommon.ssh.TunnelableSupplier;

public class TunnellingMetaStoreClientSupplier implements Supplier<CloseableMetaStoreClient> {

  private class HiveMetaStoreClientSupplier implements TunnelableSupplier<CloseableMetaStoreClient> {
    private final MetaStoreClientFactory metaStoreClientFactory;
    private final String name;
    private final HiveConf hiveConf;

    private HiveMetaStoreClientSupplier(MetaStoreClientFactory metaStoreClientFactory, HiveConf hiveConf, String name) {
      this.metaStoreClientFactory = metaStoreClientFactory;
      this.hiveConf = hiveConf;
      this.name = name;
    }

    @Override
    public CloseableMetaStoreClient get() {
      return metaStoreClientFactory.newInstance(hiveConf, name);
    }
  }

  public static class Builder {

    private String name;
    private String sshRoute;
    private int sshPort;
    private String privateKeys;
    private String knownHosts;
    private String localHost;
    private int timeout;
    private boolean strictHostKeyChecking;

    public TunnellingMetaStoreClientSupplier build(HiveConf hiveConf, MetaStoreClientFactory metaStoreClientFactory) {
      return new TunnellingMetaStoreClientSupplier(hiveConf, defaultIfBlank(name, "tunnelingMetaStoreClient"),
          defaultIfBlank(localHost, "localhost"),
          metaStoreClientFactory,
          new TunnelableFactory<CloseableMetaStoreClient>(SshSettings
              .builder()
              .withRoute(sshRoute)
              .withSshPort(sshPort)
              .withPrivateKeys(privateKeys)
              .withKnownHosts(knownHosts)
              .withSessionTimeout(timeout)
              .withStrictHostKeyChecking(strictHostKeyChecking)
              .build()));
    }

    public Builder withName(String name) {
      this.name = name;
      return this;
    }

    public Builder withRoute(String sshRoute) {
      this.sshRoute = sshRoute;
      return this;
    }

    public Builder withPort(int sshPort) {
      this.sshPort = sshPort;
      return this;
    }

    public Builder withPrivateKeys(String privateKeys) {
      this.privateKeys = privateKeys;
      return this;
    }

    public Builder withKnownHosts(String knownHosts) {
      this.knownHosts = knownHosts;
      return this;
    }

    public Builder withLocalHost(String localHost) {
      this.localHost = localHost;
      return this;
    }

    public Builder withTimeout(int timeout) {
      this.timeout = timeout;
      return this;
    }

    public Builder withStrictHostKeyChecking(String strictHostKeyChecking) {
      this.strictHostKeyChecking = "true".equals(strictHostKeyChecking) || "yes".equals(strictHostKeyChecking);
      return this;
    }
  }

  private final String localHost;
  private final String remoteHost;
  private final int remotePort;
  private final HiveConf hiveConf;
  private final String name;
  private final TunnelableFactory<CloseableMetaStoreClient> tunnelableFactory;
  private final MetaStoreClientFactory metaStoreClientFactory;

  private TunnellingMetaStoreClientSupplier(
      HiveConf hiveConf,
      String name,
      String localHost,
      MetaStoreClientFactory metaStoreClientFactory,
      TunnelableFactory<CloseableMetaStoreClient> tunnelableFactory) {
    this.hiveConf = hiveConf;
    this.name = name;
    this.tunnelableFactory = tunnelableFactory;

    URI metaStoreUri;
    try {
      metaStoreUri = new URI(hiveConf.getVar(ConfVars.METASTOREURIS));
    } catch (URISyntaxException e) {
      throw new RuntimeException(e);
    }
    remoteHost = metaStoreUri.getHost();
    remotePort = metaStoreUri.getPort();
    this.localHost = localHost;
    this.metaStoreClientFactory = metaStoreClientFactory;
  }

  @Override
  public CloseableMetaStoreClient get() {
    try {
      int localPort = getLocalPort();
      HiveConf localHiveConf = localHiveConf(hiveConf, localHost, localPort);
      HiveMetaStoreClientSupplier supplier = new HiveMetaStoreClientSupplier(metaStoreClientFactory, localHiveConf,
          name);
      return (CloseableMetaStoreClient) tunnelableFactory.wrap(supplier, MethodChecker.DEFAULT, localHost, localPort,
          remoteHost, remotePort);
    } catch (Exception e) {
      throw new MetaStoreClientException("Unable to create tunnelled HiveMetaStoreClient", e);
    }
  }

  private static int getLocalPort() {
    try (ServerSocket socket = new ServerSocket(0)) {
      return socket.getLocalPort();
    } catch (IOException | RuntimeException e) {
      throw new SshException("Unable to bind to a free localhost port", e);
    }
  }

  private static HiveConf localHiveConf(HiveConf hiveConf, String localHost, int localPort) {
    HiveConf localHiveConf = new HiveConf(hiveConf);
    String proxyMetaStoreUris = "thrift://" + localHost + ":" + localPort;
    localHiveConf.setVar(ConfVars.METASTOREURIS, proxyMetaStoreUris);
    return localHiveConf;
  }
}
