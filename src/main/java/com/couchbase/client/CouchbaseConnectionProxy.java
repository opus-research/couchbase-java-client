package com.couchbase.client;

import net.spy.memcached.ConnectionObserver;
import net.spy.memcached.FailureMode;
import net.spy.memcached.OperationFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.List;

/**
 * A {@link CouchbaseConnectableProxy} for {@link CouchbaseConnection}
 * instances.
 */
public class CouchbaseConnectionProxy
  extends CouchbaseConnectableProxy<CouchbaseConnection> {

  public CouchbaseConnectionProxy(int bufSize, CouchbaseConnectionFactory f,
    List<InetSocketAddress> a, Collection<ConnectionObserver> obs,
    FailureMode fm, OperationFactory opfactory) throws IOException {
    this(bufSize, f, a, obs, fm, opfactory, DEFAULT_POOL_SIZE);
  }

  public CouchbaseConnectionProxy(int bufSize, CouchbaseConnectionFactory f,
    List<InetSocketAddress> a, Collection<ConnectionObserver> obs,
    FailureMode fm, OperationFactory opfactory, int poolSize)
    throws IOException {
    super(bufSize, f, a, obs, fm, opfactory, poolSize);
  }

  @Override
  CouchbaseConnection getConnectableInstance(int bufSize,
    CouchbaseConnectionFactory f, List<InetSocketAddress> a,
    Collection<ConnectionObserver> obs, FailureMode fm,
    OperationFactory opfactory) throws IOException {
    return new CouchbaseConnection(bufSize, f, a, obs, fm, opfactory);
  }
}
