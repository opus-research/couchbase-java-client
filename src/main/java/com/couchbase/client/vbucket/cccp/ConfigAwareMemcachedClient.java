package com.couchbase.client.vbucket.cccp;

import net.spy.memcached.BroadcastOpFactory;
import net.spy.memcached.ConnectionFactory;
import net.spy.memcached.MemcachedClient;
import net.spy.memcached.MemcachedNode;
import net.spy.memcached.internal.OperationFuture;
import net.spy.memcached.ops.Operation;
import net.spy.memcached.ops.OperationCallback;
import net.spy.memcached.ops.OperationStatus;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;


public class ConfigAwareMemcachedClient extends MemcachedClient {

  public ConfigAwareMemcachedClient(final InetSocketAddress... ia)
    throws IOException {
    super(ia);
  }

  public ConfigAwareMemcachedClient(final List<InetSocketAddress> addrs)
    throws IOException {
    super(addrs);
  }

  public ConfigAwareMemcachedClient(final ConnectionFactory cf,
    final List<InetSocketAddress> addrs) throws IOException {
    super(cf, addrs);
  }

  /**
   * Returns a config from the first node that respons with a valid config.
   *
   * @return
   */
  public OperationFuture<String> asyncGetConfig() {
    final AtomicBoolean success = new AtomicBoolean(false);
    final CountDownLatch getLatch = new CountDownLatch(
      getNodeLocator().getAll().size());

    final OperationFuture<String> getFuture = new OperationFuture<String>(null,
      getLatch, operationTimeout, executorService) {

      private AtomicBoolean cancelled = new AtomicBoolean(false);
      private AtomicBoolean done = new AtomicBoolean(false);

      /**
       * Avoid setting the config more than once if more nodes are in play.
       *
       * @param config the config to set.
       * @param status the operation status.
       */
      @Override
      public void set(String config, OperationStatus status) {
        if (success.get() == false) {
          super.set(config, status);
          done.set(true);
        }
      }

      @Override
      public boolean cancel() {
        cancelled.set(true);
        return true;
      }

      @Override
      public boolean cancel(boolean ign) {
        cancelled.set(true);
        return true;
      }

      @Override
      public boolean isCancelled() {
        return cancelled.get();
      }

      @Override
      public boolean isDone() {
        return done.get() || cancelled.get();
      }
    };

    broadcastOp(new BroadcastOpFactory() {
      @Override
      public Operation newOp(final MemcachedNode n, final CountDownLatch l) {
        Operation op = new GetConfigOperationImpl(
          new GetConfigOperation.Callback() {
            private OperationStatus status;

            /**
             * If at least one operation returns successfully, report success.
             */
            @Override
            public void receivedStatus(final OperationStatus status) {
              this.status = status;
              if (status.isSuccess()) {
                success.set(true);
              }
            }

            @Override
            public void complete() {
              l.countDown();
              getLatch.countDown();
            }

            @Override
            public void gotData(byte[] data) {
              String hostname = ((InetSocketAddress) n.getSocketAddress()).getHostName();
              String config = new String(data).replace("$HOST", hostname);
              getFuture.set(config, status);
            }
          }
        );
        return op;
      }
    });

    return getFuture;
  }

}
