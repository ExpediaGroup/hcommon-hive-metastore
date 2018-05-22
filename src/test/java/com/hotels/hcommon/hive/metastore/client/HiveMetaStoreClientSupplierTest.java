package com.hotels.hcommon.hive.metastore.client;

import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.verify;

import org.apache.hadoop.hive.conf.HiveConf;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class HiveMetaStoreClientSupplierTest {

  private @Mock MetaStoreClientFactory metaStoreClientFactory;
  private HiveConf hiveConf = new HiveConf();
  private String name = "HiveMetaStoreClient";

  @Test
  public void get() {
    HiveMetaStoreClientSupplier metaStoreClientSupplier = new HiveMetaStoreClientSupplier(metaStoreClientFactory,
        hiveConf, name);
    metaStoreClientSupplier.get();
    verify(metaStoreClientFactory).newInstance(eq(hiveConf), eq(name));
  }
}
