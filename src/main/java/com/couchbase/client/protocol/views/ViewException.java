package com.couchbase.client.protocol.views;

import net.spy.memcached.ops.OperationErrorType;
import net.spy.memcached.ops.OperationException;

public class ViewException extends OperationException {

  private static final long serialVersionUID = 5349443788429204015L;

  public ViewException() {
    super();
  }

  public ViewException(String error, String reason) {
    super(OperationErrorType.SERVER, error + " Reason: " + reason);
  }
}
