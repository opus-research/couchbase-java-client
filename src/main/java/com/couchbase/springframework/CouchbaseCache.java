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
package com.couchbase.springframework;

import com.couchbase.client.CouchbaseClient;
import com.couchbase.client.CouchbaseConnectionFactoryBuilder;
import com.couchbase.client.TapClient;
import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.naming.ConfigurationException;
import net.spy.memcached.tapmessage.ResponseMessage;
import org.springframework.cache.Cache;

/**
 * A Couchbase based backing store for the Spring framework Cache.
 *
 * This class provides the backing store which is compatible with
 * the Spring framework Cache APIs.
 *
 * Although Spring Cache and Couchbase are both based on Key/Value
 * pairs, there are some critical differences.
 *
 * Keys can be any Object in Spring. However keys in Couchbase can
 * only be Strings and there are some associated rules with the Strings.
 * You can only use Couchbase supported Strings as keys with
 * the Couchbase-backed Spring Cache. The Couchbase Cache methods
 * use the toString() to Stringify the keys.
 *
 * To avoid namespace clashes all keys within a bucket are removed
 * when instantiating a Spring Cache.
 *
 * Consequently, each Spring Cache should be associated with a new bucket.
 *
 */

public class CouchbaseCache implements Cache {

  private String name;
  private CouchbaseClient c;
  private TapClient tapClient;

  /**
   * Instantiate a CouchbaseCache to be used as a backing store with
   * the Spring framework.
   *
   *
   * @param baseList the URI list of one or more servers from the cluster
   * @param bucketName the bucket name in the cluster you wish to use
   * @param password the password for the bucket
   * @throws IOException if connections could not be made
   */
  public CouchbaseCache(String name, List<URI> baseList,
          String bucketName, String password)
    throws IOException {
    this.name = name;
    CouchbaseConnectionFactoryBuilder cfb = new
            CouchbaseConnectionFactoryBuilder();
    c = new CouchbaseClient(cfb.buildCouchbaseConnection(baseList,
            bucketName, password));
    tapClient = new TapClient(baseList, bucketName, password);
    clear();
  }

/**
 * {@inheritDoc}.
 */
  @Override
  public void clear() {
    try {
      tapClient.tapDump("");
    } catch (IOException ex) {
      Logger.getLogger(CouchbaseCache.class.getName()).log(Level.SEVERE,
              null, ex);
    } catch (ConfigurationException ex) {
      Logger.getLogger(CouchbaseCache.class.getName()).log(Level.SEVERE,
              null, ex);
    }
    while(tapClient.hasMoreMessages()) {
      ResponseMessage response = tapClient.getNextMessage();
      if (response != null) {
        c.delete(response.getKey());
      }
    }
  }

/**
 * {@inheritDoc}.
 */
  @Override
  public void evict(Object key) {
    c.delete(key.toString());
  }

/**
 * {@inheritDoc}.
 */
  @Override
  public Cache.ValueWrapper get(final Object key) {
    return (new Cache.ValueWrapper() {
      @Override
      public Object get() {
        return c.get(key.toString());
      }
    });
  }

/**
 * {@inheritDoc}.
 */
  @Override
  public Object getNativeCache() {
    return this;
  }

/**
 * {@inheritDoc}.
 */
  @Override
  public void put(Object key, Object value) {
    try {
      c.set(key.toString(), 0, value).get();
    } catch (InterruptedException ex) {
      Logger.getLogger(CouchbaseCache.class.getName()).log(Level.SEVERE,
              null, ex);
    } catch (ExecutionException ex) {
      Logger.getLogger(CouchbaseCache.class.getName()).log(Level.SEVERE,
              null, ex);
    }
  }

/**
 * {@inheritDoc}.
 */
  @Override
  public String getName() {
    return name;
  }
}
