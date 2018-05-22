package com.hotels.hcommon.hive.metastore.client;

import org.apache.hadoop.hive.conf.HiveConf;

import com.hotels.hcommon.ssh.TunnelableSupplier;

public class HiveMetaStoreClientSupplier implements TunnelableSupplier<CloseableMetaStoreClient> {
  private final MetaStoreClientFactory metaStoreClientFactory;
  private final String name;
  private final HiveConf hiveConf;

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
