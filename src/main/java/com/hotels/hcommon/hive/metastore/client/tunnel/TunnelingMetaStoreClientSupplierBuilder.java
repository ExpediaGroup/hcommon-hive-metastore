package com.hotels.hcommon.hive.metastore.client.tunnel;

import org.apache.hadoop.hive.conf.HiveConf;

import com.hotels.hcommon.hive.metastore.client.CloseableMetaStoreClient;
import com.hotels.hcommon.hive.metastore.client.MetaStoreClientFactory;
import com.hotels.hcommon.ssh.SshSettings;
import com.hotels.hcommon.ssh.TunnelableFactory;

public class TunnelingMetaStoreClientSupplierBuilder {

  private static final String DEFAULT_NAME = "HiveMetastoreClient";
  private static final int DEFAULT_PORT = 22;
  private static final String DEFAULT_LOCALHOST = "localhost";
  private static final int DEFAULT_SESSION_TIMEOUT = 60000;
  private static final boolean DEFAULT_STRICT_HOST_KEY_CHECK = true;

  private String name = DEFAULT_NAME;
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
    return new TunnellingMetaStoreClientSupplier(hiveConf, name, localHost, metaStoreClientFactory,
        tunnelableFactory);
  }

  public TunnelingMetaStoreClientSupplierBuilder withName(String name) {
    this.name = name;
    return this;
  }

  public TunnelingMetaStoreClientSupplierBuilder withRoute(String sshRoute) {
    this.sshRoute = sshRoute;
    return this;
  }

  public TunnelingMetaStoreClientSupplierBuilder withPort(int sshPort) {
    this.sshPort = sshPort;
    return this;
  }

  public TunnelingMetaStoreClientSupplierBuilder withPrivateKeys(String privateKeys) {
    this.privateKeys = privateKeys;
    return this;
  }

  public TunnelingMetaStoreClientSupplierBuilder withKnownHosts(String knownHosts) {
    this.knownHosts = knownHosts;
    return this;
  }

  public TunnelingMetaStoreClientSupplierBuilder withLocalHost(String localHost) {
    this.localHost = localHost;
    return this;
  }

  public TunnelingMetaStoreClientSupplierBuilder withTimeout(int timeout) {
    this.timeout = timeout;
    return this;
  }

  public TunnelingMetaStoreClientSupplierBuilder withStrictHostKeyChecking(String strictHostKeyChecking) {
    this.strictHostKeyChecking = "true".equals(strictHostKeyChecking) || "yes".equals(strictHostKeyChecking);
    return this;
  }
}
