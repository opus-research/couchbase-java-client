package com.couchbase.client.vbucket.provider;

import net.spy.memcached.ops.OperationCallback;
import net.spy.memcached.ops.OperationStatus;
import net.spy.memcached.protocol.binary.OperationImpl;


public class GetConfigOperationImpl extends OperationImpl {

  private static final byte CMD = (byte) 0xb5;

  public GetConfigOperationImpl(OperationCallback cb) {
    super(CMD, 0, cb);
  }

  @Override
  public void initialize() {
    prepareBuffer("", 0, EMPTY_BYTES);
  }

  @Override
  protected void decodePayload(byte[] pl) {
    getCallback().receivedStatus(new OperationStatus(true, new String(pl)));
  }

}
