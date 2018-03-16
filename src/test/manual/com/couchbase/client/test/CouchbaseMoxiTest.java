/**
 * Copyright (C) 2006-2009 Dustin Sallings
 * Copyright (C) 2009-2011 Couchbase, Inc.
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
package com.couchbase.client.test;

import java.net.InetSocketAddress;

/**
 * Verify that couchbase client for memcached and moxi 
 * behavior is identical and the key/value pairs can be 
 * retrieved interchangeably.
 *
 * This test expects a 2-node Couchbase server with the buckets
 * being memcached buckets. Specify the IP address of one of the nodes
 * as below.
 *
 * CouchbaseMoxiTest server_address
 */

import com.couchbase.client.CouchbaseClient;

import java.net.URI;
import java.util.LinkedList;
import java.util.List;

import net.spy.memcached.MemcachedClient;


public class CouchbaseMoxiTest {
  static final long START = 1;
  static final long MAX_VALUE = 100;
  static final int TTL = 120;
  
  public static void main(String args[]) {
	MemcachedClient c=null;
        CouchbaseClient cbc=null;
        Boolean fail = false;
        
        List<URI> uris = new LinkedList<URI>();

        if (args.length != 1) {
            System.err.println("usage: server_address");
            System.exit(1);
        }

	try {
            c = new MemcachedClient(new 
                InetSocketAddress(args[1], 11211));
            URI base = new 
                URI(String.format("http://%s:8091/pools", args[1]));
            uris.add(base);
            cbc = new CouchbaseClient(uris, "default", "", "");                
        } catch (Exception e) {
          e.printStackTrace();
        }
	
        for (long key = START ; key <= MAX_VALUE; key++) {
            String myKey = String.format("Moxi%010dcbc", key);
	    c.set(myKey, TTL, myKey);
	    if (!cbc.get(myKey).equals(myKey)) {
              System.out.println("Moxi and cbc don't match " + myKey);
              fail = true;
            }
            c.delete(myKey);
            myKey = String.format("cbc%010dMoxi", key);
            cbc.set(myKey, TTL, myKey);
            if (!c.get(myKey).equals(myKey)) {
              System.out.println("cbc and Moxi don't match " + myKey);
              fail = true;
            }
            cbc.delete(myKey);
        }
        if (fail) {
          System.out.println("Couchbase Client and Moxi don't match");
        }
        else {
          System.out.println("Couchbase Client and Moxi matched from "
             + START + " to " + MAX_VALUE);
        }
        c.shutdown();
        cbc.shutdown();
  }
  
}
