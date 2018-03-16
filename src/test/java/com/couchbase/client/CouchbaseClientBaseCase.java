/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.couchbase.client;

import java.util.concurrent.TimeUnit;
import net.spy.memcached.ClientBaseCase;

/**
 *
 * @author ingenthr
 */
public class CouchbaseClientBaseCase extends ClientBaseCase {
  
  @Override
  protected void tearDown() throws Exception {
    // Shut down, null things out. Error tests have
    // unpredictable timing issues.  See test from Spymemcached
    // net.spy.memcached.ClientBaseCase
    client.shutdown(200, TimeUnit.MILLISECONDS);
    client = null;
    System.gc();
  }
  
}
