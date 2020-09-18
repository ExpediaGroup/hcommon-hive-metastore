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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import com.jcraft.jsch.JSchException;

import com.hotels.hcommon.hive.metastore.exception.MetaStoreClientException;
import com.hotels.hcommon.hive.metastore.client.api.CloseableMetaStoreClient;
import com.hotels.hcommon.hive.metastore.client.closeable.CloseableMetaStoreClientFactory;
import com.hotels.hcommon.ssh.MethodChecker;
import com.hotels.hcommon.ssh.TunnelableFactory;
import com.hotels.hcommon.ssh.TunnelableSupplier;


@RunWith(MockitoJUnitRunner.class)
public class TunnellingMetaStoreClientSupplierTest {

  private static final String LOCAL_HOST = "127.0.0.2";
  private static final int REMOTE_PORT = 9083;
  private static final String REMOTE_HOST = "emrmaster";
  private static final String NAME = "name";

  private @Mock TunnelableFactory<CloseableMetaStoreClient> tunnelableFactory;
  private @Mock CloseableMetaStoreClientFactory metaStoreClientFactory;
  private @Mock CloseableMetaStoreClient metaStoreClient;

  private final HiveConf hiveConf = new HiveConf();
  private TunnellingMetaStoreClientSupplier supplier;

  @Before
  public void init() {
    hiveConf.setVar(HiveConf.ConfVars.METASTOREURIS, "thrift://" + REMOTE_HOST + ":" + REMOTE_PORT);
  }

  @Before
  public void injectMocks() throws Exception {
    when(metaStoreClientFactory.newInstance(eq(hiveConf), eq(NAME))).thenReturn(metaStoreClient);
  }

  @Test
  public void newInstance() throws Exception {
    supplier = new TunnellingMetaStoreClientSupplier(hiveConf, NAME, LOCAL_HOST, metaStoreClientFactory,
        tunnelableFactory);
    supplier.get();
    verify(tunnelableFactory).wrap(any(TunnelableSupplier.class), any(MethodChecker.class), eq(LOCAL_HOST), anyInt(),
        eq(REMOTE_HOST), eq(REMOTE_PORT));
  }

  @Test(expected = RuntimeException.class)
  public void invalidURI() {
    hiveConf.setVar(HiveConf.ConfVars.METASTOREURIS, "#invalid://#host:port");
    supplier = new TunnellingMetaStoreClientSupplier(hiveConf, NAME, LOCAL_HOST, metaStoreClientFactory,
        tunnelableFactory);
  }

  @Test(expected = MetaStoreClientException.class)
  public void metaException() throws MetaException, IOException {
    when(tunnelableFactory.wrap(any(TunnelableSupplier.class), any(MethodChecker.class), eq(LOCAL_HOST), anyInt(),
        eq(REMOTE_HOST), eq(REMOTE_PORT))).thenThrow(MetaStoreClientException.class);

    supplier = new TunnellingMetaStoreClientSupplier(hiveConf, NAME, LOCAL_HOST, metaStoreClientFactory,
        tunnelableFactory);
    supplier.get();
  }


  @Test(expected = MetaStoreClientException.class)
  public void jschException() throws JSchException {
    reset(tunnelableFactory, metaStoreClientFactory);
    when(tunnelableFactory.wrap(any(TunnelableSupplier.class), any(MethodChecker.class), eq(LOCAL_HOST), anyInt(),
        eq(REMOTE_HOST), eq(REMOTE_PORT))).thenThrow(JSchException.class);

    supplier = new TunnellingMetaStoreClientSupplier(hiveConf, NAME, LOCAL_HOST, metaStoreClientFactory,
        tunnelableFactory);
    supplier.get();
  }

}
