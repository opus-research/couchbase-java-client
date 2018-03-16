package com.couchbase.client;

import com.couchbase.client.vbucket.ConfigurationException;
import com.couchbase.client.vbucket.config.Bucket;
import net.spy.memcached.BroadcastOpFactory;
import net.spy.memcached.ConnectionObserver;
import net.spy.memcached.FailureMode;
import net.spy.memcached.MemcachedConnection;
import net.spy.memcached.MemcachedNode;
import net.spy.memcached.NodeLocator;
import net.spy.memcached.OperationFactory;
import net.spy.memcached.compat.SpyObject;
import net.spy.memcached.ops.Operation;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

/**
 * A proxy that handles and multiplexes one or more {@link CouchbaseConnection}
 * objects.
 *
 * The proxy implements the same methods as the individual
 * {@link CouchbaseConnectable} instances, but distributes them as needed to
 * one, some or all of them.
 */
public abstract class CouchbaseConnectableProxy<C extends CouchbaseConnectable>
  implements CouchbaseConnectable {

  /**
   * The default number of {@link CouchbaseConnection} instances to create.
   */
  public static final int DEFAULT_POOL_SIZE = 1;

  /**
   * Optimization variable to avoid looping.
   */
  private final boolean onlyOneInPool;

  /**
   * Holds all proxied connections.
   */
  private final ArrayList<C> connections;

  /**
   * Create a new {@link CouchbaseConnectableProxy} with the same arguments
   * as for a normal {@link CouchbaseConnection}.
   *
   * This will create only one underlying {@link CouchbaseConnection}, see
   * the other constructor which allows to pass in a number as well.
   */
  public CouchbaseConnectableProxy(int bufSize, CouchbaseConnectionFactory f,
    List<InetSocketAddress> a, Collection<ConnectionObserver> obs,
    FailureMode fm, OperationFactory opfactory) throws IOException {
    this(bufSize, f, a, obs, fm, opfactory, DEFAULT_POOL_SIZE);
  }

  public CouchbaseConnectableProxy(int bufSize, CouchbaseConnectionFactory f,
    List<InetSocketAddress> a, Collection<ConnectionObserver> obs,
    FailureMode fm, OperationFactory opfactory, int poolSize)
    throws IOException {
    connections = new ArrayList<C>(poolSize);

    for (int i = 0; i < poolSize; i++) {
      try {
        connections.add(getConnectableInstance(bufSize, f, a, obs, fm, opfactory));
      } catch(Exception ex) {
        getLogger().warn("Could not initialize one of the " +
          "connection pool objects.");
      }
    }

    if (connections.isEmpty()) {
      throw new ConfigurationException("Could not initialize the connection " +
        "pool objects.");
    }

    onlyOneInPool = connections.size() == 1;
  }

  /**
   * Returns the connectable instance of the specified subtype.
   *
   * @return the instance to use.
   */
  abstract C getConnectableInstance(int bufSize, CouchbaseConnectionFactory f,
    List<InetSocketAddress> a, Collection<ConnectionObserver> obs,
    FailureMode fm, OperationFactory opfactory) throws IOException;

  @Override
  public void addOperation(final String key, final Operation o) {
    forOne(new ConnectionCallable<Void>() {
      @Override
      public Void call(CouchbaseConnectable c) {
        c.addOperation(key, o);
        return null;
      }
    });
  }

  @Override
  public void addOperations(final Map<MemcachedNode, Operation> ops) {
    forOne(new ConnectionCallable<Void>() {
      @Override
      public Void call(CouchbaseConnectable c) {
        c.addOperations(ops);
        return null;
      }
    });
  }

  @Override
  public void enqueueOperation(final String key, final Operation o) {
    forOne(new ConnectionCallable<Void>() {
      @Override
      public Void call(CouchbaseConnectable c) {
        c.enqueueOperation(key, o);
        return null;
      }
    });
  }

  @Override
  public void insertOperation(final MemcachedNode node, final Operation o) {
    forOne(new ConnectionCallable<Void>() {
      @Override
      public Void call(CouchbaseConnectable c) {
        c.insertOperation(node, o);
        return null;
      }
    });
  }

  @Override
  public CountDownLatch broadcastOperation(final BroadcastOpFactory of) {
    return forOneWithResult(new ConnectionCallable<CountDownLatch>() {
      @Override
      public CountDownLatch call(CouchbaseConnectable c) {
        return c.broadcastOperation(of);
      }
    });
  }

  @Override
  public CountDownLatch broadcastOperation(final BroadcastOpFactory of,
    final Collection<MemcachedNode> nodes) {
    return forOneWithResult(new ConnectionCallable<CountDownLatch>() {
      @Override
      public CountDownLatch call(CouchbaseConnectable c) {
        return c.broadcastOperation(of, nodes);
      }
    });
  }

  @Override
  public void run() {
    forAll(new ConnectionCallable<Void>() {
      @Override
      public Void call(CouchbaseConnectable c) {
        c.run();
        return null;
      }
    });
  }

  @Override
  public void handleIO() throws IOException {
    forAll(new ConnectionCallable<Void>() {
      @Override
      public Void call(CouchbaseConnectable c) {
        try {
          c.handleIO();
        } catch (IOException ex) {
          throw new RuntimeException(ex);
        }
        return null;
      }
    });
  }

  @Override
  public boolean addObserver(final ConnectionObserver obs) {
    List<Boolean> result = forAllWithResult(new ConnectionCallable<Boolean>() {
      @Override
      public Boolean call(CouchbaseConnectable c) {
        return c.addObserver(obs);
      }
    });

    for (Boolean res : result) {
      if (!res) {
        return false;
      }
    }
    return true;
  }

  @Override
  public boolean removeObserver(final ConnectionObserver obs) {
    List<Boolean> result = forAllWithResult(new ConnectionCallable<Boolean>() {
      @Override
      public Boolean call(CouchbaseConnectable c) {
        return c.removeObserver(obs);
      }
    });

    for (Boolean res : result) {
      if (!res) {
        return false;
      }
    }
    return true;
  }

  /**
   * Return the {@link NodeLocator} of one connection.
   *
   * Note that we return the first locator, because they are expected all
   * to be the same.
   *
   * @return a node locator.
   */
  @Override
  public NodeLocator getLocator() {
    return connections.get(0).getLocator();
  }

  @Override
  public void shutdown() throws IOException {
    forAll(new ConnectionCallable<Void>() {
      @Override
      public Void call(CouchbaseConnectable c) {
        try {
          c.shutdown();
        } catch (IOException ex) {
          throw new RuntimeException(ex);
        }
        return null;
      }
    });
  }

  @Override
  public String connectionsStatus() {
    List<String> result = forAllWithResult(new ConnectionCallable<String>() {
      @Override
      public String call(CouchbaseConnectable c) {
        return c.connectionsStatus();
      }
    });

    StringBuilder builder = new StringBuilder();
    for (String r : result) {
      builder.append(r);
      builder.append(";");
    }
    return builder.toString();
  }

  @Override
  public void reconfigure(final Bucket bucket) {
    forAll(new ConnectionCallable<Void>() {
      @Override
      public Void call(CouchbaseConnectable c) {
        c.reconfigure(bucket);
        return null;
      }
    });
  }

  /**
   * Run the given task on all connection instances.
   *
   * @param task the task to run
   * @param <T> the generic return value expected
   * @return a list of returned values for all connection instances.
   * @throws Exception
   */
  private <T> List<T> forAllWithResult(ConnectionCallable<T> task) {
    try {
      List<T> results = new ArrayList<T>();
      if (onlyOneInPool) {
        results.add(task.call(connections.get(0)));
      } else {
        int connSize = connections.size();
        for (int i = 0; i < connSize; i++) {
          results.add(task.call(connections.get(i)));
        }
      }
      return results;
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }

  /**
   * Run the given task and do not care about the responses.
   *
   * @param task the task to run
   * @throws Exception
   */
  private void forAll(ConnectionCallable<Void> task) {
    try {
      if (onlyOneInPool) {
        task.call(connections.get(0));
      } else {
        int connSize = connections.size();
        for (int i = 0; i < connSize; i++) {
          task.call(connections.get(i));
        }
      }
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }

  /**
   * Run the task on one (potentially random) connectable.
   *
   * @param task the task to dispatch.
   */
  private void forOne(ConnectionCallable<Void> task) {
    task.call(getNextConnectable());
  }

  /**
   * Run the task on one (potentially random) connectable with return value.
   *
   * @param task the task to dispatch.
   * @param <T> the return type.
   * @return the result of the callable.
   */
  private <T> T forOneWithResult(ConnectionCallable<T> task) {
    return task.call(getNextConnectable());
  }

  /**
   * Find the next best connectable to dispatch into.
   *
   * @return a running connectable.
   */
  private CouchbaseConnectable getNextConnectable() {
    if (onlyOneInPool) {
      return connections.get(0);
    } else {
      // FIXME
      return connections.get(0);
    }
  }

  /**
   * Defines a simple callable interface to be called for every connectable.
   *
   * @param <T> the return value, use Void if not needed.
   */
  interface ConnectionCallable<T> {
    T call(CouchbaseConnectable connectable);
  }

}
