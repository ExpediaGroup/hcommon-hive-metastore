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
package com.hotels.hcommon.hive.metastore.client.closeable;

import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.verify;

import java.lang.reflect.Method;

import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.thrift.TApplicationException;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.runners.MockitoJUnitRunner;
import org.mockito.stubbing.Answer;

import com.hotels.hcommon.hive.metastore.compatibility.HiveMetaStoreClientCompatibility;

@RunWith(MockitoJUnitRunner.class)
public class CloseableMetaStoreClientInvocationHandlerTest {

  private CloseableMetaStoreClientInvocationHandler invocationHandler;
  private @Mock IMetaStoreClient client;
  private @Mock HiveMetaStoreClientCompatibility compatibility;


  @Test
  public void typical() throws Throwable {
    invocationHandler = new CloseableMetaStoreClientInvocationHandler(client, compatibility);
    Class<?> clazz = Class.forName(IMetaStoreClient.class.getName());
    Method method = clazz.getMethod("getAllDatabases");
    invocationHandler.invoke(null, method, null);
    verify(client).getAllDatabases();
  }

  @Test
  public void invokeCompatibilityWhenTApplicationExceptionIsThrown() throws Throwable {
    Class<?> clazz = Class.forName(IMetaStoreClient.class.getName());
    Method method = clazz.getMethod("getTable", String.class, String.class);
    String dbName = "db";
    String tableName = "table";


    IMetaStoreClient exceptionThrowingClient = Mockito.mock(IMetaStoreClient.class, new Answer() {
      @Override
      public Object answer(InvocationOnMock invocation) throws Throwable {
        throw new TApplicationException();
      }
    });

    invocationHandler = new CloseableMetaStoreClientInvocationHandler(exceptionThrowingClient, compatibility);
    invocationHandler.invoke(null, method, new String[] { dbName, tableName });
    verify(compatibility).getTable(eq(dbName), eq(tableName));
  }

  @Test(expected = RuntimeException.class)
  public void dontInvokeCompatibilityWhenExceptionOtherThanTApplicationExceptionIsThrown() throws Throwable {
    Class<?> clazz = Class.forName(IMetaStoreClient.class.getName());
    Method method = clazz.getMethod("getTable", String.class, String.class);
    String dbName = "db";
    String tableName = "table";


    IMetaStoreClient exceptionThrowingClient = Mockito.mock(IMetaStoreClient.class, new Answer() {
      @Override
      public Object answer(InvocationOnMock invocation) throws Throwable {
        throw new RuntimeException();
      }
    });

    invocationHandler = new CloseableMetaStoreClientInvocationHandler(exceptionThrowingClient, compatibility);
    invocationHandler.invoke(null, method, new String[] { dbName, tableName });
  }
}
