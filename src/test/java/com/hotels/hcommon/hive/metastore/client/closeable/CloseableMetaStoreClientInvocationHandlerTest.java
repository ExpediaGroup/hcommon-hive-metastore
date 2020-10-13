/**
 * Copyright (C) 2018-2020 Expedia, Inc.
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

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.lang.reflect.Method;

import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.thrift.TApplicationException;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.junit.MockitoJUnitRunner;
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
  public void invokeCompatibilityWhenGetTableTApplicationExceptionIsThrown() throws Throwable {
    Class<?> clazz = Class.forName(IMetaStoreClient.class.getName());
    Method method = clazz.getMethod("getTable", String.class, String.class);
    String dbName = "db";
    String tableName = "table";

    IMetaStoreClient exceptionThrowingClient = Mockito.mock(IMetaStoreClient.class, new Answer<Void>() {
      @Override
      public Void answer(InvocationOnMock invocation) throws Throwable {
        throw new TApplicationException();
      }
    });

    invocationHandler = new CloseableMetaStoreClientInvocationHandler(exceptionThrowingClient, compatibility);
    invocationHandler.invoke(null, method, new String[] { dbName, tableName });
    verify(compatibility).getTable(eq(dbName), eq(tableName));
  }

  @Test
  public void invokeCompatibilityWhenTableExistsTApplicationExceptionIsThrown() throws Throwable {
    Class<?> clazz = Class.forName(IMetaStoreClient.class.getName());
    Method method = clazz.getMethod("tableExists", String.class, String.class);
    String dbName = "db";
    String tableName = "table";

    IMetaStoreClient exceptionThrowingClient = Mockito.mock(IMetaStoreClient.class, new Answer<Void>() {
      @Override
      public Void answer(InvocationOnMock invocation) throws Throwable {
        throw new TApplicationException();
      }
    });

    invocationHandler = new CloseableMetaStoreClientInvocationHandler(exceptionThrowingClient, compatibility);
    invocationHandler.invoke(null, method, new String[] { dbName, tableName });
    verify(compatibility).tableExists(eq(dbName), eq(tableName));
  }

  @Test(expected = NoSuchObjectException.class)
  public void delegateThrowsTException() throws Throwable {
    Class<?> clazz = Class.forName(IMetaStoreClient.class.getName());
    Method method = clazz.getMethod("getTable", String.class, String.class);
    String dbName = "db";
    String tableName = "table";

    IMetaStoreClient exceptionThrowingClient = Mockito.mock(IMetaStoreClient.class, new Answer<Void>() {
      @Override
      public Void answer(InvocationOnMock invocation) throws Throwable {
        throw new NoSuchObjectException();
      }
    });

    invocationHandler = new CloseableMetaStoreClientInvocationHandler(exceptionThrowingClient, compatibility);
    invocationHandler.invoke(null, method, new String[] { dbName, tableName });
  }

  @Test(expected = NoSuchObjectException.class)
  public void compatabilityThrowsTException() throws Throwable {
    Class<?> clazz = Class.forName(IMetaStoreClient.class.getName());
    Method method = clazz.getMethod("getTable", String.class, String.class);
    String dbName = "db";
    String tableName = "table";

    IMetaStoreClient exceptionThrowingClient = Mockito.mock(IMetaStoreClient.class, new Answer<Void>() {
      @Override
      public Void answer(InvocationOnMock invocation) throws Throwable {
        throw new TApplicationException();
      }
    });

    when(compatibility.getTable(dbName, tableName)).thenThrow(new NoSuchObjectException("This should be thrown"));

    invocationHandler = new CloseableMetaStoreClientInvocationHandler(exceptionThrowingClient, compatibility);
    invocationHandler.invoke(null, method, new String[] { dbName, tableName });
  }

  @Test
  public void delegateAndCompatabilityThrowTApplicationException() throws Throwable {
    Class<?> clazz = Class.forName(IMetaStoreClient.class.getName());
    Method method = clazz.getMethod("getTable", String.class, String.class);
    String dbName = "db";
    String tableName = "table";

    final TApplicationException original = new TApplicationException("original");
    IMetaStoreClient exceptionThrowingClient = Mockito.mock(IMetaStoreClient.class, new Answer<Void>() {
      @Override
      public Void answer(InvocationOnMock invocation) throws Throwable {
        throw original;
      }
    });

    when(compatibility.getTable(dbName, tableName)).thenThrow(new TApplicationException("This should not be thrown"));

    invocationHandler = new CloseableMetaStoreClientInvocationHandler(exceptionThrowingClient, compatibility);
    try {
      invocationHandler.invoke(null, method, new String[] { dbName, tableName });
      fail("Exception should have been thrown");
    } catch (TApplicationException e) {
      assertThat(e, is(original));
    }
  }

  @Test
  public void invocationExceptionOnCompatibilityLayerIsIgnoredOriginalExceptionShouldBeThrown() throws Throwable {
    Class<?> clazz = Class.forName(IMetaStoreClient.class.getName());
    // This method exists in the delegate but not in the compatibility class, will result in NoSuchMethodException from the compatibility
    // layer, which should be logged and ignored
    Method method = clazz.getMethod("getAllDatabases");

    final TApplicationException original = new TApplicationException("original");

    IMetaStoreClient exceptionThrowingClient = Mockito.mock(IMetaStoreClient.class, new Answer<Void>() {
      @Override
      public Void answer(InvocationOnMock invocation) throws Throwable {
        throw original;
      }
    });

    invocationHandler = new CloseableMetaStoreClientInvocationHandler(exceptionThrowingClient, compatibility);
    try {
      invocationHandler.invoke(null, method, new Object[0]);
      fail("Exception should have been thrown");
    } catch (TApplicationException e) {
      assertThat(e, is(original));
    }
  }
}
