/**
 * Copyright (C) 2018-2019 Expedia Inc.
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

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.thrift.TApplicationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hotels.hcommon.hive.metastore.compatibility.HiveMetaStoreClientCompatibility;

class CloseableMetaStoreClientInvocationHandler implements InvocationHandler {

  private static final Logger log = LoggerFactory.getLogger(CloseableMetaStoreClientInvocationHandler.class);

  private final IMetaStoreClient delegate;
  private final HiveMetaStoreClientCompatibility compatibility;

  CloseableMetaStoreClientInvocationHandler(IMetaStoreClient delegate, HiveMetaStoreClientCompatibility compatibility) {
    this.delegate = delegate;
    this.compatibility = compatibility;
  }

  @Override
  public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
    try {
      return method.invoke(delegate, args);
    } catch (InvocationTargetException delegateException) {
      try {
        log.info("Couldn't invoke method {}", method.toGenericString());
        if (compatibility != null
            && delegateException.getCause().getClass().isAssignableFrom(TApplicationException.class)) {
          log.info("Attempting to invoke with {}", compatibility.getClass().getName());
          return invokeCompatibility(method, args);
        }
      } catch (InvocationTargetException compatibilityException) {
        if (compatibilityException.getCause().getClass().isAssignableFrom(TApplicationException.class)) {
          log
              .warn(
                  "Unable to run compatibility for metastore client method {}. Will rethrow original exception, logging exception from compatibility layer: ",
                  method.getName(), compatibilityException);
        } else {
          // compatibility worked but threw non TApplicationException, re-throwing cause.
          throw compatibilityException.getCause();
        }
      } catch (Throwable t) {
        log
            .warn(
                "Unable to run compatibility for metastore client method {}. Will rethrow original exception, logging exception from compatibility layer: ",
                method.getName(), t);
      }
      throw delegateException.getCause();
    }
  }

  private Object invokeCompatibility(Method method, Object[] args) throws Throwable {
    Class<?>[] argTypes = getTypes(args);
    Method compatibilityMethod = compatibility.getClass().getMethod(method.getName(), argTypes);
    return compatibilityMethod.invoke(compatibility, args);
  }

  private static Class<?>[] getTypes(Object[] args) {
    Class<?>[] argTypes = new Class<?>[args.length];
    for (int i = 0; i < args.length; ++i) {
      argTypes[i] = args[i].getClass();
    }
    return argTypes;
  }

}
