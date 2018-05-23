package com.hotels.hcommon.hive.metastore;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import org.junit.Test;

import com.hotels.hcommon.hive.metastore.MetaStoreUnavailableException;

public class MetaStoreUnavailableExceptionTest {
  @Test
  public void typical() {
    String message = "message";
    Throwable cause = new RuntimeException();
    MetaStoreUnavailableException exception = new MetaStoreUnavailableException(message, cause);

    assertThat(exception.getMessage(), is(message));
    assertThat(exception.getCause(), is(cause));
  }
}
