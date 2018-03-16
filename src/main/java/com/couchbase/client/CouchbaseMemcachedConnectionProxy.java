package com.couchbase.client;

import net.spy.memcached.ConnectionObserver;
import net.spy.memcached.FailureMode;
import net.spy.memcached.OperationFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.List;

/**
 * A {@link CouchbaseConnectableProxy} for {@link CouchbaseMemcachedConnection}
 * instances.
 */
public class CouchbaseMemcachedConnectionProxy
  extends CouchbaseConnectableProxy<CouchbaseMemcachedConnection> {

  public CouchbaseMemcachedConnectionProxy(int bufSize,
    CouchbaseConnectionFactory f, List<InetSocketAddress> a,
    Collection<ConnectionObserver> obs, FailureMode fm,
    OperationFactory opfactory) throws IOException {
    this(bufSize, f, a, obs, fm, opfactory, DEFAULT_POOL_SIZE);
  }

  public CouchbaseMemcachedConnectionProxy(int bufSize,
    CouchbaseConnectionFactory f, List<InetSocketAddress> a,
    Collection<ConnectionObserver> obs, FailureMode fm,
    OperationFactory opfactory, int poolSize) throws IOException {
    super(bufSize, f, a, obs, fm, opfactory, poolSize);
  }

  @Override
  CouchbaseMemcachedConnection getConnectableInstance(int bufSize,
    CouchbaseConnectionFactory f, List<InetSocketAddress> a,
    Collection<ConnectionObserver> obs, FailureMode fm,
    OperationFactory opfactory) throws IOException {
    return new CouchbaseMemcachedConnection(bufSize, f, a, obs, fm, opfactory);
  }
}
