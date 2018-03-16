/**
 * Copyright (C) 2009-2014 Couchbase, Inc.
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

package net.spy.memcached.auth;

import static org.junit.Assert.assertEquals;

import java.net.URI;
import java.util.Arrays;

import net.spy.memcached.TestConfig;

import org.junit.Test;

import com.couchbase.client.CouchbaseClient;
import com.couchbase.client.CouchbaseConnectionFactory;


/**
 * Verifies the correct auth mechanism corresponding to the correct server versions.
 */
public class AuthTest {

  /**
   * Client instance
   */
  private CouchbaseClient client;

  /**
   * Initializes the client object.
   * @throws Exception
   */
  protected void initClient() throws Exception {
    CouchbaseConnectionFactory cf = new CouchbaseConnectionFactory(Arrays.asList(
      URI.create("http://" + TestConfig.IPV4_ADDR + ":8091/pools")), "default",
      "");
    client = new CouchbaseClient((CouchbaseConnectionFactory) cf);
  }

  /**
   * Test for Auth Mechanisms.
   *
   * @pre Prepare a new instance of client and extract
   * the versions of the server from it and then check
   * for the Auth mechanism to be SASL Cram MD5 for
   * version 2.2+
   *
   * @post Asserts pass if the auth mechanism is
   * correct for the respective server version.
   * @throws Exception
   */
  @Test
  public void testAuthMech() throws Exception {
    initClient();
    String serverVersion = client.getVersions().toString();
    String authMechanism = client.listSaslMechanisms().toString();
    if(serverVersion.contains("2.0.1")){
      assertEquals(authMechanism,"[PLAIN]");
    }else if(serverVersion.contains("2.2") || serverVersion.contains("2.5")){
      assertEquals(authMechanism,"[PLAIN, CRAM-MD5]");
    }
  }
}