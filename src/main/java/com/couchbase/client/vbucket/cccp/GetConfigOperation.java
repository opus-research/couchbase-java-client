package com.couchbase.client.vbucket.cccp;

import net.spy.memcached.ops.Operation;
import net.spy.memcached.ops.OperationCallback;


public interface GetConfigOperation extends Operation {

  /**
   * Operation callback for the get config request.
   */
  interface Callback extends OperationCallback {

    /**
     * Callback for each result from a get.
     *
     * @param data the payload returned.
     */
    void gotData(byte[] data);
  }

}
