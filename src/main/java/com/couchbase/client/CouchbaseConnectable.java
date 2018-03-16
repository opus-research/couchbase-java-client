package com.couchbase.client;

import com.couchbase.client.vbucket.Reconfigurable;
import net.spy.memcached.BroadcastOpFactory;
import net.spy.memcached.ConnectionObserver;
import net.spy.memcached.MemcachedNode;
import net.spy.memcached.NodeLocator;
import net.spy.memcached.ops.Operation;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

/**
 * A marker interface to define common methods than can be issued against a
 * connection.
 *
 * This interface is in general implemented by {@link CouchbaseConnection} and
 * {@link CouchbaseMemcachedConnection}, as well as through the
 * {@link CouchbaseConnectableProxy}.
 */
interface CouchbaseConnectable extends Reconfigurable {

  void addOperation(final String key, final Operation o);
  void addOperations(final Map<MemcachedNode, Operation> ops);
  void run();
  void handleIO() throws IOException;
  boolean addObserver(ConnectionObserver obs);
  boolean removeObserver(ConnectionObserver obs);
  NodeLocator getLocator();
  void enqueueOperation(String key, Operation o);
  void insertOperation(MemcachedNode node, Operation o);
  CountDownLatch broadcastOperation(BroadcastOpFactory of);
  CountDownLatch broadcastOperation(BroadcastOpFactory of, Collection<MemcachedNode> nodes);
  void shutdown() throws IOException;
  String toString();
  String connectionsStatus();

}
