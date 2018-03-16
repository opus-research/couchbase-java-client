/**
 * Copyright (C) 2009-2012 Couchbase, Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALING
 * IN THE SOFTWARE.
 */

package com.couchbase.client;

import com.couchbase.client.internal.HttpFuture;
import com.couchbase.client.internal.ViewFuture;
import com.couchbase.client.protocol.views.DocsOperationImpl;
import com.couchbase.client.protocol.views.HttpOperation;
import com.couchbase.client.protocol.views.InvalidViewException;
import com.couchbase.client.protocol.views.NoDocsOperationImpl;
import com.couchbase.client.protocol.views.Paginator;
import com.couchbase.client.protocol.views.Query;
import com.couchbase.client.protocol.views.ReducedOperationImpl;
import com.couchbase.client.protocol.views.View;
import com.couchbase.client.protocol.views.ViewFetcherOperation;
import com.couchbase.client.protocol.views.ViewFetcherOperationImpl;
import com.couchbase.client.protocol.views.ViewOperation.ViewCallback;
import com.couchbase.client.protocol.views.ViewResponse;
import com.couchbase.client.protocol.views.ViewRow;
import com.couchbase.client.protocol.views.ViewsFetcherOperation;
import com.couchbase.client.protocol.views.ViewsFetcherOperationImpl;
import com.couchbase.client.vbucket.Reconfigurable;
import com.couchbase.client.vbucket.VBucketNodeLocator;
import com.couchbase.client.vbucket.config.Bucket;
import com.couchbase.client.vbucket.config.Config;
import com.couchbase.client.vbucket.config.ConfigType;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.logging.Level;
import java.util.logging.Logger;

import net.spy.memcached.AddrUtil;
import net.spy.memcached.BroadcastOpFactory;
import net.spy.memcached.CASResponse;
import net.spy.memcached.CASValue;
import net.spy.memcached.CachedData;
import net.spy.memcached.MemcachedClient;
import net.spy.memcached.MemcachedNode;
import net.spy.memcached.ObserveResponse;
import net.spy.memcached.OperationTimeoutException;
import net.spy.memcached.PersistTo;
import net.spy.memcached.ReplicateTo;
import net.spy.memcached.compat.CloseUtil;
import net.spy.memcached.internal.OperationFuture;
import net.spy.memcached.ops.GetlOperation;
import net.spy.memcached.ops.ObserveOperation;
import net.spy.memcached.ops.Operation;
import net.spy.memcached.ops.OperationCallback;
import net.spy.memcached.ops.OperationStatus;
import net.spy.memcached.ops.StatsOperation;
import net.spy.memcached.transcoders.Transcoder;

import org.apache.http.HttpRequest;
import org.apache.http.HttpVersion;
import org.apache.http.message.BasicHttpRequest;

/**
 * A client for Couchbase Server.
 *
 * This class acts as your main entry point while working with your Couchbase
 * cluster (if you want to work with TAP, see the TapClient instead).
 *
 * If you are working with Couchbase Server 2.0, remember to set the appropriate
 * view mode depending on your environment.
 */
public class CouchbaseClient extends MemcachedClient
  implements CouchbaseClientIF, Reconfigurable {

  private static final String MODE_PRODUCTION = "production";
  private static final String MODE_DEVELOPMENT = "development";
  private static final String DEV_PREFIX = "dev_";
  private static final String PROD_PREFIX = "";
  public static final String MODE_PREFIX;
  private static final String MODE_ERROR;

  private ViewConnection vconn = null;
  protected volatile boolean reconfiguring = false;
  private final CouchbaseConnectionFactory cbConnFactory;

  /**
   * Properties priority from highest to lowest:
   *
   * 1. Property defined in user code.
   * 2. Property defined on command line.
   * 3. Property defined in cbclient.properties.
   */
  static {
    Properties properties = new Properties(System.getProperties());
    String viewmode = properties.getProperty("viewmode", null);

    if (viewmode == null) {
      FileInputStream fs = null;
      try {
        URL url =  ClassLoader.getSystemResource("cbclient.properties");
        if (url != null) {
          fs = new FileInputStream(new File(url.getFile()));
          properties.load(fs);
        }
        viewmode = properties.getProperty("viewmode");
      } catch (IOException e) {
        // Properties file doesn't exist. Error logged later.
      } finally {
        if (fs != null) {
          CloseUtil.close(fs);
        }
      }
    }

    if (viewmode == null) {
      MODE_ERROR = "viewmode property isn't defined. Setting viewmode to"
        + " production mode";
      MODE_PREFIX = PROD_PREFIX;
    } else if (viewmode.equals(MODE_PRODUCTION)) {
      MODE_ERROR = "viewmode set to production mode";
      MODE_PREFIX = PROD_PREFIX;
    } else if (viewmode.equals(MODE_DEVELOPMENT)) {
      MODE_ERROR = "viewmode set to development mode";
      MODE_PREFIX = DEV_PREFIX;
    } else {
      MODE_ERROR = "unknown value \"" + viewmode + "\" for property viewmode"
          + " Setting to production mode";
      MODE_PREFIX = PROD_PREFIX;
    }
  }

  /**
   * Get a CouchbaseClient based on the initial server list provided.
   *
   * This constructor should be used if the bucket name is the same as the
   * username (which is normally the case). If your bucket does not have
   * a password (likely the "default" bucket), use an empty string instead.
   *
   * This method is only a convenience method so you don't have to create a
   * CouchbaseConnectionFactory for yourself.
   *
   * @param baseList the URI list of one or more servers from the cluster
   * @param bucketName the bucket name in the cluster you wish to use
   * @param pwd the password for the bucket
   * @throws IOException if connections could not be made
   * @throws ConfigurationException if the configuration provided by the server
   *           has issues or is not compatible
   */
  public CouchbaseClient(final List<URI> baseList, final String bucketName,
    final String pwd)
    throws IOException {
    this(new CouchbaseConnectionFactory(baseList, bucketName, pwd));
  }

  /**
   * Get a CouchbaseClient based on the initial server list provided.
   *
   * Currently, Couchbase Server does not support a different username than the
   * bucket name. Therefore, this method ignores the given username for now
   * but will likely use it in the future.
   *
   * This constructor should be used if the bucket name is NOT the same as the
   * username. If your bucket does not have a password (likely the "default"
   * bucket), use an empty string instead.
   *
   * This method is only a convenience method so you don't have to create a
   * CouchbaseConnectionFactory for yourself.
   *
   * @param baseList the URI list of one or more servers from the cluster
   * @param bucketName the bucket name in the cluster you wish to use
   * @param user the username for the bucket
   * @param pwd the password for the bucket
   * @throws IOException if connections could not be made
   * @throws ConfigurationException if the configuration provided by the server
   *           has issues or is not compatible
   */
  public CouchbaseClient(final List<URI> baseList, final String bucketName,
    final String user, final String pwd)
    throws IOException {
    this(new CouchbaseConnectionFactory(baseList, bucketName, pwd));
  }

  /**
   * Get a CouchbaseClient based on the settings from the given
   * CouchbaseConnectionFactory.
   *
   * If your bucket does not have a password (likely the "default" bucket), use
   * an empty string instead.
   *
   * The URI list provided here is only used during the initial connection to
   * the cluster. Afterwards, the actual cluster-map is synchronized from the
   * cluster and maintained internally by the client. This allows the client to
   * update the map as needed (when the cluster topology changes).
   *
   * Note that when specifying a ConnectionFactory you must specify a
   * BinaryConnectionFactory (which is the case if you use the
   * CouchbaseConnectionFactory). Also the ConnectionFactory's protocol and
   * locator values are always overwritten. The protocol will always be binary
   * and the locator will be chosen based on the bucket type you are connecting
   * to.
   *
   * The subscribe variable determines whether or not we will subscribe to
   * the configuration changes feed. This constructor should be used when
   * calling super from subclasses of CouchbaseClient since the subclass might
   * want to start the changes feed later.
   *
   * @param cf the ConnectionFactory to use to create connections
   * @throws IOException if connections could not be made
   * @throws ConfigurationException if the configuration provided by the server
   *           has issues or is not compatible
   */
  public CouchbaseClient(CouchbaseConnectionFactory cf)
    throws IOException {
    super(cf, AddrUtil.getAddresses(cf.getVBucketConfig().getServers()));
    cbConnFactory = cf;

    if(cf.getVBucketConfig().getConfigType() == ConfigType.COUCHBASE) {
      List<InetSocketAddress> addrs =
        AddrUtil.getAddressesFromURL(cf.getVBucketConfig().getCouchServers());
      vconn = cf.createViewConnection(addrs);
    }

    getLogger().info(MODE_ERROR);
    cf.getConfigurationProvider().subscribe(cf.getBucketName(), this);
  }

  /**
   * This method is called when there is a topology change in the cluster.
   *
   * This method is intended for internal use only.
   */
  public void reconfigure(Bucket bucket) {
    reconfiguring = true;
    if (bucket.isNotUpdating()) {
      getLogger().info("Bucket configuration is disconnected from cluster "
        + "configuration updates, attempting to reconnect.");
      CouchbaseConnectionFactory cbcf = (CouchbaseConnectionFactory)connFactory;
      cbcf.requestConfigReconnect(cbcf.getBucketName(), this);
    }
    try {
      if(vconn != null) {
        vconn.reconfigure(bucket);
      }
      if (mconn instanceof CouchbaseConnection) {
        CouchbaseConnection cbConn = (CouchbaseConnection) mconn;
        cbConn.reconfigure(bucket);
      } else {
        CouchbaseMemcachedConnection cbMConn =
          (CouchbaseMemcachedConnection) mconn;
        cbMConn.reconfigure(bucket);
      }
    } catch (IllegalArgumentException ex) {
      getLogger().warn("Failed to reconfigure client, staying with "
          + "previous configuration.", ex);
    } finally {
      reconfiguring = false;
    }
  }



  /**
   * Gets access to a view contained in a design document from the cluster.
   *
   * The purpose of a view is take the structured data stored within the
   * Couchbase Server database as JSON documents, extract the fields and
   * information, and to produce an index of the selected information.
   *
   * The result is a view on the stored data. The view that is created
   * during this process allows you to iterate, select and query the
   * information in your database from the raw data objects that have
   * been stored.
   *
   * Note that since an HttpFuture is returned, the caller must also check to
   * see if the View is null. The HttpFuture does provide a getStatus() method
   * which can be used to check whether or not the view request has been
   * successful.
   *
   * @param designDocumentName the name of the design document.
   * @param viewName the name of the view to get.
   * @return a View object from the cluster.
   * @throws InterruptedException if the operation is interrupted while in
   *           flight
   * @throws ExecutionException if an error occurs during execution
   */
  public HttpFuture<View> asyncGetView(String designDocumentName,
      final String viewName) {
    designDocumentName = MODE_PREFIX + designDocumentName;
    String bucket = ((CouchbaseConnectionFactory)connFactory).getBucketName();
    String uri = "/" + bucket + "/_design/" + designDocumentName;
    final CountDownLatch couchLatch = new CountDownLatch(1);
    final HttpFuture<View> crv = new HttpFuture<View>(couchLatch, 60000);

    final HttpRequest request =
        new BasicHttpRequest("GET", uri, HttpVersion.HTTP_1_1);
    final HttpOperation op =
        new ViewFetcherOperationImpl(request, bucket, designDocumentName,
            viewName, new ViewFetcherOperation.ViewFetcherCallback() {
              private View view = null;

              @Override
              public void receivedStatus(OperationStatus status) {
                crv.set(view, status);
              }

              @Override
              public void complete() {
                couchLatch.countDown();
              }

              @Override
              public void gotData(View v) {
                view = v;
              }
            });
    crv.setOperation(op);
    addOp(op);
    assert crv != null : "Problem retrieving view";
    return crv;
  }

  /**
   * Gets a future with a list of views for a given design document from the
   * cluster.
   *
   * The purpose of a view is take the structured data stored within the
   * Couchbase Server database as JSON documents, extract the fields and
   * information, and to produce an index of the selected information.
   *
   * The result is a view on the stored data. The view that is created
   * during this process allows you to iterate, select and query the
   * information in your database from the raw data objects that have
   * been stored.
   *
   * Note that since an HttpFuture is returned, the caller must also check to
   * see if the View is null. The HttpFuture does provide a getStatus() method
   * which can be used to check whether or not the view request has been
   * successful.
   *
   * @param designDocumentName the name of the design document.
   * @return a future containing a List of View objects from the cluster.
   */
  public HttpFuture<List<View>> asyncGetViews(String designDocumentName) {
    designDocumentName = MODE_PREFIX + designDocumentName;
    String bucket = ((CouchbaseConnectionFactory)connFactory).getBucketName();
    String uri = "/" + bucket + "/_design/" + designDocumentName;
    final CountDownLatch couchLatch = new CountDownLatch(1);
    final HttpFuture<List<View>> crv =
        new HttpFuture<List<View>>(couchLatch, 60000);

    final HttpRequest request =
        new BasicHttpRequest("GET", uri, HttpVersion.HTTP_1_1);
    final HttpOperation op = new ViewsFetcherOperationImpl(request, bucket,
        designDocumentName, new ViewsFetcherOperation.ViewsFetcherCallback() {
          private List<View> views = null;

          @Override
          public void receivedStatus(OperationStatus status) {
            crv.set(views, status);
          }

          @Override
          public void complete() {
            couchLatch.countDown();
          }

          @Override
          public void gotData(List<View> v) {
            views = v;
          }
        });
    crv.setOperation(op);
    addOp(op);
    return crv;
  }

  /**
   * Gets access to a view contained in a design document from the cluster.
   *
   * The purpose of a view is take the structured data stored within the
   * Couchbase Server database as JSON documents, extract the fields and
   * information, and to produce an index of the selected information.
   *
   * The result is a view on the stored data. The view that is created
   * during this process allows you to iterate, select and query the
   * information in your database from the raw data objects that have
   * been stored.
   *
   * @param designDocumentName the name of the design document.
   * @param viewName the name of the view to get.
   * @return a View object from the cluster.
   * @throws InvalidViewException if no design document or view was found.
   */
  public View getView(final String designDocumentName, final String viewName) {
    try {
      View view = asyncGetView(designDocumentName, viewName).get();
      if(view == null) {
        throw new InvalidViewException("Could not load view \""
          + viewName + "\" for design doc \"" + designDocumentName + "\"");
      }
      return view;
    } catch (InterruptedException e) {
      throw new RuntimeException("Interrupted getting views", e);
    } catch (ExecutionException e) {
      throw new RuntimeException("Failed getting views", e);
    }
  }

  /**
   * Gets a list of views for a given design document from the cluster.
   *
   * @param designDocumentName the name of the design document.
   * @return a list of View objects from the cluster.
   * @throws InvalidViewException if no design document or view was found.
   */
  public List<View> getViews(final String designDocumentName) {
    try {
      List<View> views = asyncGetViews(designDocumentName).get();
      if(views == null) {
        throw new InvalidViewException("Could not load views for design doc \""
          + designDocumentName + "\"");
      }
      return views;
    } catch (InterruptedException e) {
      throw new RuntimeException("Interrupted getting views", e);
    } catch (ExecutionException e) {
      throw new RuntimeException("Failed getting views", e);
    }
  }

  public HttpFuture<ViewResponse> asyncQuery(View view, Query query) {
    if (query.willReduce()) {
      return asyncQueryAndReduce(view, query);
    } else if (query.willIncludeDocs()) {
      return asyncQueryAndIncludeDocs(view, query);
    } else {
      return asyncQueryAndExcludeDocs(view, query);
    }
  }

  /**
   * Asynchronously queries a Couchbase view and returns the result.
   * The result can be accessed row-wise via an iterator. This
   * type of query will return the view result along with all of the documents
   * for each row in the query.
   *
   * @param view the view to run the query against.
   * @param query the type of query to run against the view.
   * @return a Future containing the results of the query.
   */
  private HttpFuture<ViewResponse> asyncQueryAndIncludeDocs(View view,
      Query query) {
    assert view != null : "Who passed me a null view";
    assert query != null : "who passed me a null query";
    String viewUri = view.getURI();
    String queryToRun = query.toString();
    assert viewUri != null : "view URI seems to be null";
    assert queryToRun != null  : "query seems to be null";
    String uri = viewUri + queryToRun;
    getLogger().info("lookin for:" + uri);
    final CountDownLatch couchLatch = new CountDownLatch(1);
    final ViewFuture crv = new ViewFuture(couchLatch, 60000);

    final HttpRequest request =
        new BasicHttpRequest("GET", uri, HttpVersion.HTTP_1_1);
    final HttpOperation op = new DocsOperationImpl(request, new ViewCallback() {
      private ViewResponse vr = null;

      @Override
      public void receivedStatus(OperationStatus status) {
        if (vr != null) {
          Collection<String> ids = new LinkedList<String>();
          Iterator<ViewRow> itr = vr.iterator();
          while (itr.hasNext()) {
            ids.add(itr.next().getId());
          }
          crv.set(vr, asyncGetBulk(ids), status);
        } else {
          crv.set(null, null, status);
        }
      }

      @Override
      public void complete() {
        couchLatch.countDown();
      }

      @Override
      public void gotData(ViewResponse response) {
        vr = response;
      }
    });
    crv.setOperation(op);
    addOp(op);
    return crv;
  }

  /**
   * Asynchronously queries a Couchbase view and returns the result.
   * The result can be accessed row-wise via an iterator. This
   * type of query will return the view result but will not
   * get the documents associated with each row of the query.
   *
   * @param view the view to run the query against.
   * @param query the type of query to run against the view.
   * @return a Future containing the results of the query.
   */
  private HttpFuture<ViewResponse> asyncQueryAndExcludeDocs(View view,
      Query query) {
    String uri = view.getURI() + query.toString();
    final CountDownLatch couchLatch = new CountDownLatch(1);
    final HttpFuture<ViewResponse> crv =
        new HttpFuture<ViewResponse>(couchLatch, 60000);

    final HttpRequest request =
        new BasicHttpRequest("GET", uri, HttpVersion.HTTP_1_1);
    final HttpOperation op =
        new NoDocsOperationImpl(request, new ViewCallback() {
          private ViewResponse vr = null;

          @Override
          public void receivedStatus(OperationStatus status) {
            crv.set(vr, status);
          }

          @Override
          public void complete() {
            couchLatch.countDown();
          }

          @Override
          public void gotData(ViewResponse response) {
            vr = response;
          }
        });
    crv.setOperation(op);
    addOp(op);
    return crv;
  }

  /**
   * Asynchronously queries a Couchbase view and returns the result.
   * The result can be accessed row-wise via an iterator.
   *
   * @param view the view to run the query against.
   * @param query the type of query to run against the view.
   * @return a Future containing the results of the query.
   */
  private HttpFuture<ViewResponse> asyncQueryAndReduce(final View view,
      final Query query) {
    if (!view.hasReduce()) {
      throw new RuntimeException("This view doesn't contain a reduce function");
    }
    String uri = view.getURI() + query.toString();
    final CountDownLatch couchLatch = new CountDownLatch(1);
    final HttpFuture<ViewResponse> crv =
        new HttpFuture<ViewResponse>(couchLatch, 60000);

    final HttpRequest request =
        new BasicHttpRequest("GET", uri, HttpVersion.HTTP_1_1);
    final HttpOperation op =
        new ReducedOperationImpl(request, new ViewCallback() {
          private ViewResponse vr = null;

          @Override
          public void receivedStatus(OperationStatus status) {
            crv.set(vr, status);
          }

          @Override
          public void complete() {
            couchLatch.countDown();
          }

          @Override
          public void gotData(ViewResponse response) {
            vr = response;
          }
        });
    crv.setOperation(op);
    addOp(op);
    return crv;
  }

  /**
   * Queries a Couchbase view and returns the result.
   * The result can be accessed row-wise via an iterator.
   * This type of query will return the view result along
   * with all of the documents for each row in
   * the query.
   *
   * @param view the view to run the query against.
   * @param query the type of query to run against the view.
   * @return a ViewResponseWithDocs containing the results of the query.
   */
  public ViewResponse query(View view, Query query) {
    try {
      return asyncQuery(view, query).get();
    } catch (InterruptedException e) {
      throw new RuntimeException("Interrupted while accessing the view", e);
    } catch (ExecutionException e) {
      throw new RuntimeException("Failed to access the view", e);
    }
  }

  /**
   * A paginated query allows the user to get the results of a large query in
   * small chunks allowing for better performance. The result allows you
   * to iterate through the results of the query and when you get to the end
   * of the current result set the client will automatically fetch the next set
   * of results.
   *
   * @param view the view to query against.
   * @param query the query for this request.
   * @param docsPerPage the amount of documents per page.
   * @return A Paginator (iterator) to use for reading the results of the query.
   */
  public Paginator paginatedQuery(View view, Query query, int docsPerPage) {
    return new Paginator(this, view, query, docsPerPage);
  }

  /**
   * Adds an operation to the queue where it waits to be sent to Couchbase. This
   * function is for internal use only.
   */
  public void addOp(final HttpOperation op) {
    if(vconn != null) {
      vconn.checkState();
      vconn.addOp(op);
    }
  }


  /**
   * Gets and locks the given key asynchronously. By default the maximum allowed
   * timeout is 30 seconds. Timeouts greater than this will be set to 30
   * seconds.
   *
   * @param key the key to fetch and lock
   * @param exp the amount of time the lock should be valid for in seconds.
   * @param tc the transcoder to serialize and unserialize value
   * @return a future that will hold the return value of the fetch
   * @throws IllegalStateException in the rare circumstance where queue is too
   *           full to accept any more requests
   */
  public <T> OperationFuture<CASValue<T>> asyncGetAndLock(final String key,
      int exp, final Transcoder<T> tc) {
    final CountDownLatch latch = new CountDownLatch(1);
    final OperationFuture<CASValue<T>> rv =
        new OperationFuture<CASValue<T>>(key, latch, operationTimeout);

    Operation op = opFact.getl(key, exp, new GetlOperation.Callback() {
      private CASValue<T> val = null;

      public void receivedStatus(OperationStatus status) {
        if (!status.isSuccess()) {
          val = new CASValue<T>(-1, null);
        }
        rv.set(val, status);
      }

      public void gotData(String k, int flags, long cas, byte[] data) {
        assert key.equals(k) : "Wrong key returned";
        assert cas > 0 : "CAS was less than zero:  " + cas;
        val =
            new CASValue<T>(cas, tc.decode(new CachedData(flags, data, tc
                .getMaxSize())));
      }

      public void complete() {
        latch.countDown();
      }
    });
    rv.setOperation(op);
    mconn.enqueueOperation(key, op);
    return rv;
  }

  /**
   * Get and lock the given key asynchronously and decode with the default
   * transcoder. By default the maximum allowed timeout is 30 seconds. Timeouts
   * greater than this will be set to 30 seconds.
   *
   * @param key the key to fetch and lock
   * @param exp the amount of time the lock should be valid for in seconds.
   * @return a future that will hold the return value of the fetch
   * @throws IllegalStateException in the rare circumstance where queue is too
   *           full to accept any more requests
   */
  public OperationFuture<CASValue<Object>> asyncGetAndLock(final String key,
      int exp) {
    return asyncGetAndLock(key, exp, transcoder);
  }

  /**
   * Getl with a single key. By default the maximum allowed timeout is 30
   * seconds. Timeouts greater than this will be set to 30 seconds.
   *
   * @param key the key to get and lock
   * @param exp the amount of time the lock should be valid for in seconds.
   * @param tc the transcoder to serialize and unserialize value
   * @return the result from the cache (null if there is none)
   * @throws OperationTimeoutException if the global operation timeout is
   *           exceeded
   * @throws IllegalStateException in the rare circumstance where queue is too
   *           full to accept any more requests
   */
  public <T> CASValue<T> getAndLock(String key, int exp, Transcoder<T> tc) {
    try {
      return asyncGetAndLock(key, exp, tc).get(operationTimeout,
          TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      throw new RuntimeException("Interrupted waiting for value", e);
    } catch (ExecutionException e) {
      throw new RuntimeException("Exception waiting for value", e);
    } catch (TimeoutException e) {
      throw new OperationTimeoutException("Timeout waiting for value", e);
    }
  }

  /**
   * Get and lock with a single key and decode using the default transcoder. By
   * default the maximum allowed timeout is 30 seconds. Timeouts greater than
   * this will be set to 30 seconds.
   *
   * @param key the key to get and lock
   * @param exp the amount of time the lock should be valid for in seconds.
   * @return the result from the cache (null if there is none)
   * @throws OperationTimeoutException if the global operation timeout is
   *           exceeded
   * @throws IllegalStateException in the rare circumstance where queue is too
   *           full to accept any more requests
   */
  public CASValue<Object> getAndLock(String key, int exp) {
    return getAndLock(key, exp, transcoder);
  }

  /**
   * Unlock the given key asynchronously from the cache.
   *
   * @param key the key to unlock
   * @param casId the CAS identifier
   * @param tc the transcoder to serialize and unserialize value
   * @return whether or not the operation was performed
   * @throws IllegalStateException in the rare circumstance where queue is too
   *           full to accept any more requests
   */
  public <T> OperationFuture<Boolean> asyncUnlock(final String key,
          long casId, final Transcoder<T> tc) {
    final CountDownLatch latch = new CountDownLatch(1);
    final OperationFuture<Boolean> rv = new OperationFuture<Boolean>(key,
            latch, operationTimeout);
    Operation op = opFact.unlock(key, casId, new OperationCallback() {

      @Override
      public void receivedStatus(OperationStatus s) {
        rv.set(s.isSuccess(), s);
      }

      @Override
      public void complete() {
        latch.countDown();
      }
    });
    rv.setOperation(op);
    mconn.enqueueOperation(key, op);
    return rv;
  }

  /**
   * Unlock the given key asynchronously from the cache with the default
   * transcoder.
   *
   * @param key the key to unlock
   * @param casId the CAS identifier
   * @return whether or not the operation was performed
   * @throws IllegalStateException in the rare circumstance where queue is too
   *           full to accept any more requests
   */
  public OperationFuture<Boolean> asyncUnlock(final String key,
          long casId) {
    return asyncUnlock(key, casId, transcoder);
  }

  /**
   * Unlock the given key synchronously from the cache.
   *
   * @param key the key to unlock
   * @param casId the CAS identifier
   * @param tc the transcoder to serialize and unserialize value
   * @return whether or not the operation was performed
   * @throws IllegalStateException in the rare circumstance where queue is too
   *           full to accept any more requests
   */
  public <T> Boolean unlock(final String key,
          long casId, final Transcoder<T> tc) {
    try {
      return asyncUnlock(key, casId, tc).get(operationTimeout,
          TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      throw new RuntimeException("Interrupted waiting for value", e);
    } catch (ExecutionException e) {
      throw new RuntimeException("Exception waiting for value", e);
    } catch (TimeoutException e) {
      throw new OperationTimeoutException("Timeout waiting for value", e);
    }

  }

  /**
   * Unlock the given key synchronously from the cache with the default
   * transcoder.
   *
   * @param key the key to unlock
   * @param casId the CAS identifier
   * @return whether or not the operation was performed
   * @throws IllegalStateException in the rare circumstance where queue is too
   *           full to accept any more requests
   */
  public Boolean unlock(final String key,
          long casId) {
    return unlock(key, casId, transcoder);
  }

  /**
   * Set a value with durability options.
   *
   * To make sure that a value is stored the way you want it to in the
   * cluster, you can use the PersistTo and ReplicateTo arguments. The
   * operation will block until the desired state is satisfied or
   * otherwise an exception is raised. There are many reasons why this could
   * happen, the more frequent ones are as follows:
   *
   * - The given replication settings are invalid.
   * - The operation could not be completed within the timeout.
   * - Something goes wrong and a cluster failover is triggered.
   *
   * The client does not attempt to guarantee the given durability
   * constraints, it just reports whether the operation has been completed
   * or not. If it is not achieved, it is the responsibility of the
   * application code using this API to re-retrieve the items to verify
   * desired state, redo the operation or both.
   *
   * Note that even if an exception during the observation is raised,
   * this doesn't mean that the operation has failed. A normal set()
   * operation is initiated and after the OperationFuture has returned,
   * the key itself is observed with the given durability options (watch
   * out for Observed*Exceptions) in this case.
   *
   * @param key the key to store.
   * @param exp the expiry value to use.
   * @param value the value of the key.
   * @param req the amount of nodes the item should be persisted to before
   *            returning.
   * @param rep the amount of nodes the item should be replicated to before
   *            returning.
   * @return the future result of the set operation.
   */
  public OperationFuture<Boolean> set(String key, int exp,
          String value, PersistTo req, ReplicateTo rep) {

    OperationFuture<Boolean> setOp = set(key, exp, value);

    boolean setStatus = false;

    try {
      setStatus = setOp.get();
    } catch (InterruptedException e) {
      setOp.set(false, new OperationStatus(false, "Set get timed out"));
    } catch (ExecutionException e) {
      setOp.set(false, new OperationStatus(false, "Set get "
              + "execution exception "));
    }
    if (!setStatus) {
      return setOp;
    }
    try {
      observePoll(key, setOp.getCas(), req, rep, false);
      setOp.set(true, setOp.getStatus());
    } catch (ObservedException e) {
      setOp.set(false, new OperationStatus(false, e.getMessage()));
    } catch (ObservedTimeoutException e) {
      setOp.set(false, new OperationStatus(false, e.getMessage()));
    } catch (ObservedModifiedException e) {
      setOp.set(false, new OperationStatus(false, e.getMessage()));
    }
    return setOp;
  }

  /**
   * Set a value with durability options.
   *
   * This is a shorthand method so that you only need to provide a
   * PersistTo value if you don't care if the value is already replicated.
   * A PersistTo.TWO durability setting implies a replication to at least
   * one node.
   *
   * For more information on how the durability options work, see the docblock
   * for the set() operation with both PersistTo and ReplicateTo settings.
   *
   * @param key the key to store.
   * @param exp the expiry value to use.
   * @param value the value of the key.
   * @param req the amount of nodes the item should be persisted to before
   *            returning.
   * @return the future result of the set operation.
   */
  public OperationFuture<Boolean> set(String key, int exp,
          String value, PersistTo req) {
    return set(key, exp, value, req, ReplicateTo.ZERO);
  }

  /**
   * Set a value with durability options.
   *
   * This method allows you to express durability at the replication level
   * only and is the functional equivalent of PersistTo.ZERO.
   *
   * A common use case for this would be to achieve good insert-performance
   * and at the same time making sure that the data is at least replicated
   * to the given amount of nodes to provide a better level of data safety.
   *
   * For more information on how the durability options work, see the docblock
   * for the set() operation with both PersistTo and ReplicateTo settings.
   *
   * @param key the key to store.
   * @param exp the expiry value to use.
   * @param value the value of the key.
   * @param rep the amount of nodes the item should be replicated to before
   *            returning.
   * @return the future result of the set operation.
   */
  public OperationFuture<Boolean> set(String key, int exp,
          String value, ReplicateTo rep) {
    return set(key, exp, value, PersistTo.ZERO, rep);
  }

  /**
   * Add a value with durability options.
   *
   * To make sure that a value is stored the way you want it to in the
   * cluster, you can use the PersistTo and ReplicateTo arguments. The
   * operation will block until the desired state is satisfied or
   * otherwise an exception is raised. There are many reasons why this could
   * happen, the more frequent ones are as follows:
   *
   * - The given replication settings are invalid.
   * - The operation could not be completed within the timeout.
   * - Something goes wrong and a cluster failover is triggered.
   *
   * The client does not attempt to guarantee the given durability
   * constraints, it just reports whether the operation has been completed
   * or not. If it is not achieved, it is the responsibility of the
   * application code using this API to re-retrieve the items to verify
   * desired state, redo the operation or both.
   *
   * Note that even if an exception during the observation is raised,
   * this doesn't mean that the operation has failed. A normal add()
   * operation is initiated and after the OperationFuture has returned,
   * the key itself is observed with the given durability options (watch
   * out for Observed*Exceptions) in this case.
   *
   * @param key the key to store.
   * @param exp the expiry value to use.
   * @param value the value of the key.
   * @param req the amount of nodes the item should be persisted to before
   *            returning.
   * @param rep the amount of nodes the item should be replicated to before
   *            returning.
   * @return the future result of the add operation.
   */
  public OperationFuture<Boolean> add(String key, int exp,
          String value, PersistTo req, ReplicateTo rep) {

    OperationFuture<Boolean> addOp = add(key, exp, value);

    boolean addStatus = false;

    try {
      addStatus = addOp.get();
    } catch (InterruptedException e) {
      addOp.set(false, new OperationStatus(false, "Add get timed out"));
    } catch (ExecutionException e) {
      addOp.set(false, new OperationStatus(false, "Add get "
              + "execution exception "));
    }
    if (!addStatus) {
      return addOp;
    }
    try {
      observePoll(key, addOp.getCas(), req, rep, false);
      addOp.set(true, addOp.getStatus());
    } catch (ObservedException e) {
      addOp.set(false, new OperationStatus(false, e.getMessage()));
    } catch (ObservedTimeoutException e) {
      addOp.set(false, new OperationStatus(false, e.getMessage()));
    } catch (ObservedModifiedException e) {
      addOp.set(false, new OperationStatus(false, e.getMessage()));
    }
    return addOp;
  }

  /**
   * Add a value with durability options.
   *
   * This is a shorthand method so that you only need to provide a
   * PersistTo value if you don't care if the value is already replicated.
   * A PersistTo.TWO durability setting implies a replication to at least
   * one node.
   *
   * For more information on how the durability options work, see the docblock
   * for the add() operation with both PersistTo and ReplicateTo settings.
   *
   * @param key the key to store.
   * @param exp the expiry value to use.
   * @param value the value of the key.
   * @param req the amount of nodes the item should be persisted to before
   *            returning.
   * @return the future result of the add operation.
   */
  public OperationFuture<Boolean> add(String key, int exp,
          String value, PersistTo req) {
    return add(key, exp, value, req, ReplicateTo.ZERO);
  }


  /**
   * Add a value with durability options.
   *
   * This method allows you to express durability at the replication level
   * only and is the functional equivalent of PersistTo.ZERO.
   *
   * A common use case for this would be to achieve good insert-performance
   * and at the same time making sure that the data is at least replicated
   * to the given amount of nodes to provide a better level of data safety.
   *
   * For more information on how the durability options work, see the docblock
   * for the add() operation with both PersistTo and ReplicateTo settings.
   *
   * @param key the key to store.
   * @param exp the expiry value to use.
   * @param value the value of the key.
   * @param rep the amount of nodes the item should be replicated to before
   *            returning.
   * @return the future result of the add operation.
   */
  public OperationFuture<Boolean> add(String key, int exp,
          String value, ReplicateTo rep) {
    return add(key, exp, value, PersistTo.ZERO, rep);
  }

  /**
   * Replace a value with durability options.
   *
   * To make sure that a value is stored the way you want it to in the
   * cluster, you can use the PersistTo and ReplicateTo arguments. The
   * operation will block until the desired state is satisfied or
   * otherwise an exception is raised. There are many reasons why this could
   * happen, the more frequent ones are as follows:
   *
   * - The given replication settings are invalid.
   * - The operation could not be completed within the timeout.
   * - Something goes wrong and a cluster failover is triggered.
   *
   * The client does not attempt to guarantee the given durability
   * constraints, it just reports whether the operation has been completed
   * or not. If it is not achieved, it is the responsibility of the
   * application code using this API to re-retrieve the items to verify
   * desired state, redo the operation or both.
   *
   * Note that even if an exception during the observation is raised,
   * this doesn't mean that the operation has failed. A normal replace()
   * operation is initiated and after the OperationFuture has returned,
   * the key itself is observed with the given durability options (watch
   * out for Observed*Exceptions) in this case.
   *
   * @param key the key to store.
   * @param exp the expiry value to use.
   * @param value the value of the key.
   * @param req the amount of nodes the item should be persisted to before
   *            returning.
   * @param rep the amount of nodes the item should be replicated to before
   *            returning.
   * @return the future result of the replace operation.
   */
  public OperationFuture<Boolean> replace(String key, int exp,
          String value, PersistTo req, ReplicateTo rep) {

    OperationFuture<Boolean> replaceOp = replace(key, exp, value);

    boolean replaceStatus = false;

    try {
      replaceStatus = replaceOp.get();
    } catch (InterruptedException e) {
      replaceOp.set(false, new OperationStatus(false, "Replace get timed out"));
    } catch (ExecutionException e) {
      replaceOp.set(false, new OperationStatus(false, "Replace get "
              + "execution exception "));
    }
    if (!replaceStatus) {
      return replaceOp;
    }
    try {
      observePoll(key, replaceOp.getCas(), req, rep, false);
      replaceOp.set(true, replaceOp.getStatus());
    } catch (ObservedException e) {
      replaceOp.set(false, new OperationStatus(false, e.getMessage()));
    } catch (ObservedTimeoutException e) {
      replaceOp.set(false, new OperationStatus(false, e.getMessage()));
    } catch (ObservedModifiedException e) {
      replaceOp.set(false, new OperationStatus(false, e.getMessage()));
    }
    return replaceOp;

  }

  /**
   * Replace a value with durability options.
   *
   * This is a shorthand method so that you only need to provide a
   * PersistTo value if you don't care if the value is already replicated.
   * A PersistTo.TWO durability setting implies a replication to at least
   * one node.
   *
   * For more information on how the durability options work, see the docblock
   * for the replace() operation with both PersistTo and ReplicateTo settings.
   *
   * @param key the key to store.
   * @param exp the expiry value to use.
   * @param value the value of the key.
   * @param req the amount of nodes the item should be persisted to before
   *            returning.
   * @return the future result of the replace operation.
   */
  public OperationFuture<Boolean> replace(String key, int exp,
          String value, PersistTo req) {
    return replace(key, exp, value, req, ReplicateTo.ZERO);
  }

  /**
   * Replace a value with durability options.
   *
   * This method allows you to express durability at the replication level
   * only and is the functional equivalent of PersistTo.ZERO.
   *
   * A common use case for this would be to achieve good insert-performance
   * and at the same time making sure that the data is at least replicated
   * to the given amount of nodes to provide a better level of data safety.
   *
   * For more information on how the durability options work, see the docblock
   * for the replace() operation with both PersistTo and ReplicateTo settings.
   *
   * @param key the key to store.
   * @param exp the expiry value to use.
   * @param value the value of the key.
   * @param rep the amount of nodes the item should be replicated to before
   *            returning.
   * @return the future result of the replace operation.
   */
  public OperationFuture<Boolean> replace(String key, int exp,
          String value, ReplicateTo rep) {
    return replace(key, exp, value, PersistTo.ZERO, rep);
  }

  /**
   * Set a value with a CAS and durability options.
   *
   * To make sure that a value is stored the way you want it to in the
   * cluster, you can use the PersistTo and ReplicateTo arguments. The
   * operation will block until the desired state is satisfied or
   * otherwise an exception is raised. There are many reasons why this could
   * happen, the more frequent ones are as follows:
   *
   * - The given replication settings are invalid.
   * - The operation could not be completed within the timeout.
   * - Something goes wrong and a cluster failover is triggered.
   *
   * The client does not attempt to guarantee the given durability
   * constraints, it just reports whether the operation has been completed
   * or not. If it is not achieved, it is the responsibility of the
   * application code using this API to re-retrieve the items to verify
   * desired state, redo the operation or both.
   *
   * Note that even if an exception during the observation is raised,
   * this doesn't mean that the operation has failed. A normal asyncCAS()
   * operation is initiated and after the OperationFuture has returned,
   * the key itself is observed with the given durability options (watch
   * out for Observed*Exceptions) in this case.
   *
   * @param key the key to store.
   * @param cas the CAS value to use.
   * @param value the value of the key.
   * @param req the amount of nodes the item should be persisted to before
   *            returning.
   * @param rep the amount of nodes the item should be replicated to before
   *            returning.
   * @return the future result of the CAS operation.
   */
  public CASResponse cas(String key, long cas,
          String value, PersistTo req, ReplicateTo rep) {

    OperationFuture<CASResponse> casOp = asyncCAS(key, cas, value);
    CASResponse casr = null;
    try {
      casr = casOp.get();
    } catch (InterruptedException e) {
      casr = CASResponse.EXISTS;
    } catch (ExecutionException e) {
      casr = CASResponse.EXISTS;
    }
    if (casr != CASResponse.OK) {
      return casr;
    }
    try {
      observePoll(key, casOp.getCas(), req, rep, false);
    } catch (ObservedException e) {
      casr = CASResponse.OBSERVE_ERROR_IN_ARGS;
    } catch (ObservedTimeoutException e) {
      casr = CASResponse.OBSERVE_TIMEOUT;
    } catch (ObservedModifiedException e) {
      casr = CASResponse.OBSERVE_MODIFIED;
    }
    return casr;
  }

  /**
   * Set a value with a CAS and durability options.
   *
   * This is a shorthand method so that you only need to provide a
   * PersistTo value if you don't care if the value is already replicated.
   * A PersistTo.TWO durability setting implies a replication to at least
   * one node.
   *
   * For more information on how the durability options work, see the docblock
   * for the cas() operation with both PersistTo and ReplicateTo settings.
   *
   * @param key the key to store.
   * @param cas the CAS value to use.
   * @param value the value of the key.
   * @param req the amount of nodes the item should be persisted to before
   *            returning.
   * @return the future result of the CAS operation.
   */
  public CASResponse cas(String key, long cas,
          String value, PersistTo req) {
    return cas(key, cas, value, req, ReplicateTo.ZERO);
  }

  /**
   * Set a value with a CAS and durability options.
   *
   * This method allows you to express durability at the replication level
   * only and is the functional equivalent of PersistTo.ZERO.
   *
   * A common use case for this would be to achieve good insert-performance
   * and at the same time making sure that the data is at least replicated
   * to the given amount of nodes to provide a better level of data safety.
   *
   * For more information on how the durability options work, see the docblock
   * for the cas() operation with both PersistTo and ReplicateTo settings.
   *
   * @param key the key to store.
   * @param cas the CAS value to use.
   * @param value the value of the key.
   * @param rep the amount of nodes the item should be replicated to before
   *            returning.
   * @return the future result of the CAS operation.
   */
  public CASResponse cas(String key, long cas,
          String value, ReplicateTo rep) {
    return cas(key, cas, value, PersistTo.ZERO, rep);
  }

  /**
   * Observe a key with a associated CAS.
   *
   * This method allows you to check immediately on the state of a given
   * key/CAS combination. It is normally used by higher-level methods when
   * used in combination with durability constraints (ReplicateTo,
   * PersistTo), but can also be used separately.
   *
   * @param key the key to observe.
   * @param cas the CAS of the key (0 will ignore it).
   * @return ObserveReponse the Response on master and replicas.
   * @throws IllegalStateException in the rare circumstance where queue is too
   *           full to accept any more requests.
   */
  public Map<MemcachedNode, ObserveResponse> observe(final String key,
      final long cas) {
    Config cfg = ((CouchbaseConnectionFactory) connFactory).getVBucketConfig();
    VBucketNodeLocator locator = ((VBucketNodeLocator)
        ((CouchbaseConnection) mconn).getLocator());

    final int vb = locator.getVBucketIndex(key);
    List<MemcachedNode> bcastNodes = new ArrayList<MemcachedNode>();

    bcastNodes.add(locator.getServerByIndex(cfg.getMaster(vb)));
    for (int i = 1; i <= cfg.getReplicasCount(); i++) {
      bcastNodes.add(locator.getServerByIndex(cfg.getReplica(vb, i-1)));
    }

    final Map<MemcachedNode, ObserveResponse> response =
        new HashMap<MemcachedNode, ObserveResponse>();

    CountDownLatch blatch = broadcastOp(new BroadcastOpFactory() {
      public Operation newOp(final MemcachedNode n,
          final CountDownLatch latch) {
        return opFact.observe(key, cas, vb, new ObserveOperation.Callback() {

          public void receivedStatus(OperationStatus s) {
          }

          public void gotData(String key, long retCas, MemcachedNode node,
              ObserveResponse or) {
            if (cas != retCas) {
              response.put(node, ObserveResponse.MODIFIED);
            } else {
              response.put(node, or);
            }
          }
          public void complete() {
            latch.countDown();
          }
        });
      }
    }, bcastNodes);
    try {
      blatch.await(operationTimeout, TimeUnit.MILLISECONDS);
      return response;
    } catch (InterruptedException e) {
      throw new RuntimeException("Interrupted waiting for value", e);
    }
  }

  /**
   * Gets the number of vBuckets that are contained in the cluster. This
   * function is for internal use only and should rarely be since there
   * are few use cases in which it is necessary.
   */
  @Override
  public int getNumVBuckets() {
    return ((CouchbaseConnectionFactory)connFactory).getVBucketConfig()
      .getVbucketsCount();
  }

  @Override
  public boolean shutdown(long timeout, TimeUnit unit) {
    boolean shutdownResult = false;
    try {
      shutdownResult = super.shutdown(timeout, unit);
      CouchbaseConnectionFactory cf = (CouchbaseConnectionFactory) connFactory;
      cf.getConfigurationProvider().shutdown();
      if(vconn != null) {
        vconn.shutdown();
      }
    } catch (IOException ex) {
      Logger.getLogger(
         CouchbaseClient.class.getName()).log(Level.SEVERE,
            "Unexpected IOException in shutdown", ex);
      throw new RuntimeException(null, ex);
    }
    return shutdownResult;
  }

  /**
   * Poll and observe a key with the given CAS and persist settings.
   *
   * Based on the given persistence and replication settings, it observes the
   * key and raises an exception if a timeout has been reached. This method is
   * normally utilized through higher-level methods but can also be used
   * directly.
   *
   * If persist is null, it will default to PersistTo.ZERO and if replicate is
   * null, it will default to ReplicateTo.ZERO. This is the default behavior
   * and is the same as not observing at all.
   *
   * @param key the key to observe.
   * @param cas the CAS value for the key.
   * @param persist the persistence settings.
   * @param replicate the replication settings.
   * @param isDelete if the key is to be deleted.
   */
  public void observePoll(String key, long cas, PersistTo persist,
      ReplicateTo replicate, boolean isDelete) {
    boolean persistMaster = false;
    if(persist == null) {
      persist = PersistTo.ZERO;
    }
    if(replicate == null) {
      replicate = ReplicateTo.ZERO;
    }
    int persistReplica = persist.getValue();
    int replicateTo = replicate.getValue();
    int obsPolls = 0;
    int obsPollMax = cbConnFactory.getObsPollMax();
    long obsPollInterval = cbConnFactory.getObsPollInterval();

    Config cfg = ((CouchbaseConnectionFactory) connFactory).getVBucketConfig();
    VBucketNodeLocator locator = ((VBucketNodeLocator)
        ((CouchbaseConnection) mconn).getLocator());

    int replicaCount = Math.min(locator.getAll().size() - 1,
          cfg.getReplicasCount());

    if (replicateTo > replicaCount) {
      throw new ObservedException("Requested replication to " + replicateTo
          + " node(s), but only " + replicaCount + " are avaliable");
    } else if (persistReplica > replicaCount + 1) {
      throw new ObservedException("Requested persistence to " + persistReplica
          + " node(s), but only " + (replicaCount + 1) + " are available.");
    }

    int replicaPersistedTo = 0;
    int replicatedTo = 0;
    boolean persistedMaster = false;
    while(replicateTo > replicatedTo || persistReplica - 1 > replicaPersistedTo
        || (!persistedMaster && persistMaster)) {
      if (++obsPolls >= obsPollMax) {
        long timeTried = obsPollMax * obsPollInterval;
        TimeUnit tu = TimeUnit.MILLISECONDS;
        throw new ObservedTimeoutException("Observe Timeout - Polled"
            + " Unsuccessfully for at least " + tu.toSeconds(timeTried)
            + " seconds.");
      }
      Map<MemcachedNode, ObserveResponse> response = observe(key, cas);

      int vb = locator.getVBucketIndex(key);
      MemcachedNode master = locator.getServerByIndex(cfg.getMaster(vb));

      replicaPersistedTo = 0;
      replicatedTo = 0;
      persistedMaster = false;
      for (Entry<MemcachedNode, ObserveResponse> r : response.entrySet()) {
        boolean isMaster = r.getKey() == master ? true : false;
        if (isMaster && r.getValue() == ObserveResponse.MODIFIED) {
          throw new ObservedModifiedException("Key was modified");
        }
        if (!isDelete) {
          if (r.getValue() == ObserveResponse.FOUND_NOT_PERSISTED) {
            replicatedTo++;
          }
          if (r.getValue() == ObserveResponse.FOUND_PERSISTED) {
            replicatedTo++;
            if (isMaster) {
              persistedMaster = true;
            } else {
              replicaPersistedTo++;
            }
          }
        } else {
          if (r.getValue() == ObserveResponse.NOT_FOUND_NOT_PERSISTED) {
            replicatedTo++;
          }
          if (r.getValue() == ObserveResponse.NOT_FOUND_PERSISTED) {
            replicatedTo++;
            replicaPersistedTo++;
            if (isMaster) {
              persistedMaster = true;
            } else {
              replicaPersistedTo++;
            }
          }
        }
      }
      try {
        Thread.sleep(obsPollInterval);
      } catch (InterruptedException e) {
        getLogger().error("Interrupted while in observe loop.", e);
        throw new ObservedException("Observe was Interrupted ");
      }
    }
  }

  public OperationFuture<Map<String, String>> getKeyStats(String key) {
    final CountDownLatch latch = new CountDownLatch(1);
    final OperationFuture<Map<String, String>> rv =
        new OperationFuture<Map<String, String>>(key, latch, operationTimeout);
    Operation op = opFact.keyStats(key, new StatsOperation.Callback() {
      private Map<String, String> stats = new HashMap<String, String>();

      public void gotStat(String name, String val) {
        stats.put(name, val);
      }

      public void receivedStatus(OperationStatus status) {
        rv.set(stats, status);
      }

      public void complete() {
        latch.countDown();
      }
    });
    rv.setOperation(op);
    mconn.enqueueOperation(key, op);
    return rv;
  }
}
