package com.couchbase.client.http;

import com.couchbase.client.ViewConnection;
import com.couchbase.client.protocol.views.HttpOperation;

public class RequeueOpCallback {

  private final ViewConnection conn;

  public RequeueOpCallback(ViewConnection vc) {
    conn = vc;
  }

  public void invoke(HttpOperation op) {
    conn.addOp(op);
  }
}
