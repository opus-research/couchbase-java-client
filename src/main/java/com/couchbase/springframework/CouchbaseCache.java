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
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.naming.ConfigurationException;
import net.spy.memcached.tapmessage.ResponseMessage;
import org.springframework.cache.Cache;

/**
 * A CouchbaseCache.
 */
public class CouchbaseCache implements Cache {

  private String name;
  private CouchbaseClient c;
  private TapClient tapClient;

  public CouchbaseCache(String name, String hostname,
          String bucket, String password)
    throws URISyntaxException, IOException {
    this.name = name;
    URI local = new URI(String.format("http://%s:8091/pools", hostname));
    List<URI> baseURIs = new ArrayList<URI>();
    baseURIs.add(local);
    CouchbaseConnectionFactoryBuilder cfb = new
            CouchbaseConnectionFactoryBuilder();
    c = new CouchbaseClient(cfb.buildCouchbaseConnection(baseURIs,
            bucket, password));
    tapClient = new TapClient(baseURIs, bucket, password);
    clear();
  }

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

  @Override
  public void evict(Object key) {
    c.delete(key.toString());
  }

  @Override
  public Cache.ValueWrapper get(final Object key) {
    return (new Cache.ValueWrapper() {
      @Override
      public Object get() {
        return c.get(key.toString());
      }
    });
  }

  @Override
  public Object getNativeCache() {
    return this;
  }

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

  @Override
  public String getName() {
    return name;
  }
}
