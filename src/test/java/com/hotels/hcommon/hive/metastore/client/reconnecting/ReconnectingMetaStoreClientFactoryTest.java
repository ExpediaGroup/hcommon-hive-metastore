package com.hotels.hcommon.hive.metastore.client.reconnecting;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

import java.util.concurrent.TimeUnit;

import org.apache.hadoop.hive.conf.HiveConf;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import com.sun.jdi.InvocationException;

import com.hotels.hcommon.hive.metastore.MetaStoreClientException;

@RunWith(MockitoJUnitRunner.class)
public class ReconnectingMetaStoreClientFactoryTest {

  private final HiveConf conf = new HiveConf();
  private final String name = "name";
  private final int retries = 5;

  private ReconnectingMetaStoreClientFactory metaStoreClientFactory;

  @Before
  public void init() {
    conf.setVar(HiveConf.ConfVars.METASTOREURIS, "hrift://ip-12-34-567-891.us-east-1.compute.internal:9083");
    conf.setIntVar(HiveConf.ConfVars.METASTORETHRIFTCONNECTIONRETRIES, 1);
    conf.setTimeVar(HiveConf.ConfVars.METASTORE_CLIENT_CONNECT_RETRY_DELAY, 10L, TimeUnit.SECONDS);
    metaStoreClientFactory = spy(new ReconnectingMetaStoreClientFactory(conf, name, retries));
  }

  @Test
  public void typical() throws Exception {
    metaStoreClientFactory.newInstance();
    verify(metaStoreClientFactory).getReconectingMetaStoreClientInvocationHandler(eq(conf), eq(name), eq(retries));
    verify(metaStoreClientFactory).getProxyInstance(any(ReconnectingMetaStoreClientInvocationHandler.class));
  }

  @Test(expected = MetaStoreClientException.class)
  public void failure() throws Exception {
    doThrow(InvocationException.class).when(
        metaStoreClientFactory).getProxyInstance(any(ReconnectingMetaStoreClientInvocationHandler.class));
    metaStoreClientFactory.newInstance();
  }

}
