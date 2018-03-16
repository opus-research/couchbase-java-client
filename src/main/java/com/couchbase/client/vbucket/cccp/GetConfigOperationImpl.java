package com.couchbase.client.vbucket.cccp;

import net.spy.memcached.ops.OperationCallback;
import net.spy.memcached.protocol.binary.OperationImpl;


public class GetConfigOperationImpl extends OperationImpl
  implements GetConfigOperation {

  /**
   * Represents the CMD_GET_CLUSTER_CONFIG command.
   */
  private static final byte CMD = (byte) 0xb5;

  /**
   * No need for opaque here.
   */
  private static final int OPAQUE = 0;

  public GetConfigOperationImpl(OperationCallback callback) {
    super(CMD, OPAQUE, callback);
  }

  @Override
  public void initialize() {
    prepareBuffer("", 0, EMPTY_BYTES);
  }

  @Override
  protected void decodePayload(byte[] pl) {
    GetConfigOperation.Callback gcb = (GetConfigOperation.Callback) getCallback();
    gcb.gotData(pl);
    getCallback().receivedStatus(STATUS_OK);
  }
}
