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

import java.io.IOException;
import java.net.ServerSocket;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;

import edu.umd.cs.findbugs.annotations.NonNull;

import com.google.common.base.Supplier;

import com.hotels.hcommon.hive.metastore.MetaStoreClientException;
import com.hotels.hcommon.hive.metastore.client.HiveMetaStoreClientSupplier;
import com.hotels.hcommon.hive.metastore.client.api.CloseableMetaStoreClient;
import com.hotels.hcommon.hive.metastore.client.api.MetaStoreClientFactory;
import com.hotels.hcommon.ssh.MethodChecker;
import com.hotels.hcommon.ssh.SshException;
import com.hotels.hcommon.ssh.TunnelableFactory;

public class TunnellingMetaStoreClientSupplier implements Supplier<CloseableMetaStoreClient> {

  private final HiveConf hiveConf;
  private final String name;
  private final String localHost;
  private final String remoteHost;
  private final int remotePort;
  private final TunnelableFactory<CloseableMetaStoreClient> tunnelableFactory;
  private final MetaStoreClientFactory metaStoreClientFactory;

  TunnellingMetaStoreClientSupplier(
      @NonNull HiveConf hiveConf,
      @NonNull String name,
      @NonNull String localHost,
      @NonNull MetaStoreClientFactory metaStoreClientFactory,
      @NonNull TunnelableFactory<CloseableMetaStoreClient> tunnelableFactory) {
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

  private static HiveConf localHiveConf(HiveConf hiveConf, String localHost, int localPort) {
    HiveConf localHiveConf = new HiveConf(hiveConf);
    String proxyMetaStoreUris = "thrift://" + localHost + ":" + localPort;
    localHiveConf.setVar(ConfVars.METASTOREURIS, proxyMetaStoreUris);
    return localHiveConf;
  }

  private static int getLocalPort() {
    try (ServerSocket socket = new ServerSocket(0)) {
      return socket.getLocalPort();
    } catch (IOException | RuntimeException e) {
      throw new SshException("Unable to bind to a free localhost port", e);
    }
  }
}
