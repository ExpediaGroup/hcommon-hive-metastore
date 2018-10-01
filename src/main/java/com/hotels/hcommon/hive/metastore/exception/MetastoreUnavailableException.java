package com.hotels.hcommon.hive.metastore.exception;

public class MetastoreUnavailableException extends RuntimeException {

  private static final long serialVersionUID = 1L;

  public MetastoreUnavailableException(String message) {
    super(message);
  }

  public MetastoreUnavailableException(String message, Throwable cause) {
    super(message, cause);
  }

}
