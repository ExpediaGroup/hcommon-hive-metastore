package com.hotels.hcommon.hive.metastore.client.closeable;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.thrift.TApplicationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hotels.hcommon.hive.metastore.compatibility.HiveMetaStoreClientCompatibility;

public class CloseableMetaStoreClientInvocationHandler implements InvocationHandler {

  private static final Logger log = LoggerFactory.getLogger(CloseableMetaStoreClientInvocationHandler.class);

  private final IMetaStoreClient delegate;
  private final HiveMetaStoreClientCompatibility compatibility;

  CloseableMetaStoreClientInvocationHandler(
      IMetaStoreClient delegate,
      HiveMetaStoreClientCompatibility compatibility) {
    this.delegate = delegate;
    this.compatibility = compatibility;
  }

  @Override
  public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
    try {
      return method.invoke(delegate, args);
    } catch (InvocationTargetException e) {
      try {
        if (compatibility != null && e.getCause().getClass().isAssignableFrom(TApplicationException.class)) {
          return invokeCompatibility(method, args);
        }
      } catch (Throwable t) {
        log.warn("Unable to run compatibility for metastore client method {}. Will rethrow original exception: ",
            method.getName(), t);
      }
      throw e.getCause();
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
