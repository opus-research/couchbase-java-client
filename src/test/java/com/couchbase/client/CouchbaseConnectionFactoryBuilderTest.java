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
package com.couchbase.client;

import java.io.IOException;
import java.net.URI;
import java.util.Arrays;
import java.util.List;
import net.spy.memcached.TestConfig;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 * Test for Properties in the CouchbaseConnectionFactoryBuilder.
 * To run the test, create a cbclient.properties in the work directory
 * with the following entries.
 *
 * obsPollMax=99
 * obsPollInterval=444
 * opTimeout=999
 * timeoutExceptionThreshold=999
 *
 * ant -DobsPollInterval=444 test
 *
 * or set the same properties in the command line.
 *
 * These properties are applicable only when using the
 * CouchbaseConnectionFactoryBuilder. To generate clients with
 * default values, CouchbaseClient or CouchbaseConnectionFactory should
 * be used.
 */
public class CouchbaseConnectionFactoryBuilderTest {

  private List<URI> uris = Arrays.asList(
      URI.create("http://" + TestConfig.IPV4_ADDR + ":8091/pools"));

  public CouchbaseConnectionFactoryBuilderTest() {
  }

  @BeforeClass
  public static void setUpClass() {
  }

  @AfterClass
  public static void tearDownClass() {
  }

  @Before
  public void setUp() {
  }

  @After
  public void tearDown() {
  }

  /**
   * Test of setObsPollInterval method, of class
   * CouchbaseConnectionFactoryBuilder.
   */
  @Test
  public void testSetObsPollInterval() throws IOException {
    long timeInterval = 600L;
    CouchbaseConnectionFactoryBuilder instance =
      new CouchbaseConnectionFactoryBuilder();
    assertEquals("Did the test run with obsPollInterval=444",
            444L, instance.getObsPollInterval());
    CouchbaseConnectionFactoryBuilder instanceResult =
      instance.setObsPollInterval(timeInterval);
    assertEquals("Failed to set observe poll interval.", 600L,
      instanceResult.getObsPollInterval());
    instance.buildCouchbaseConnection(uris, "default", "");
  }

  /**
   * Test of setObsPollMax method, of class CouchbaseConnectionFactoryBuilder.
   */
  @Test
  public void testSetObsPollMax() throws IOException {
    int maxPoll = 40;
    CouchbaseConnectionFactoryBuilder instance =
      new CouchbaseConnectionFactoryBuilder();
    assertEquals("Did the test run with ObsPollMax=99",
            99L, instance.getObsPollMax());
    CouchbaseConnectionFactoryBuilder instanceResult
      = instance.setObsPollMax(maxPoll);
    assertEquals(maxPoll, instanceResult.getObsPollMax());
    instance.buildCouchbaseConnection(uris, "default", "");
  }

  /**
   * Test of testopTimeout method, of class
   * CouchbaseConnectionFactoryBuilder.
   */
  @Test
  public void testopTimeOut() throws IOException {
    int opTimeout = 999;
    CouchbaseConnectionFactoryBuilder instance =
      new CouchbaseConnectionFactoryBuilder();
    CouchbaseConnectionFactory cf =
            instance.buildCouchbaseConnection(uris, "default", "", "");
    assertEquals("Did the test run with opTimeout=999",
            999L, cf.getOperationTimeout());
    CouchbaseConnectionFactoryBuilder instanceResult
      = (CouchbaseConnectionFactoryBuilder)
            instance.setOpTimeout(opTimeout);
    assertEquals(opTimeout,
            instanceResult.build().getOperationTimeout());
    instance.buildCouchbaseConnection(uris, "default", "");
  }

  /**
   * Test of testTimeoutExceptionThreshold method, of class
   * CouchbaseConnectionFactoryBuilder.
   */
  @Test
  public void testTimeoutExceptionThreshold() throws IOException {
    int timeoutExceptionThreshold = 9999;
    CouchbaseConnectionFactoryBuilder instance =
      new CouchbaseConnectionFactoryBuilder();
    CouchbaseConnectionFactory cf =
            instance.buildCouchbaseConnection(uris, "default", "", "");
    assertEquals("Did the test run with timeoutExceptionThreshold=999",
            999L, cf.getTimeoutExceptionThreshold());
    CouchbaseConnectionFactoryBuilder instanceResult
      = (CouchbaseConnectionFactoryBuilder)
            instance.setTimeoutExceptionThreshold(
            timeoutExceptionThreshold + 2);
    assertEquals(timeoutExceptionThreshold,
            instanceResult.build().getTimeoutExceptionThreshold());
    instance.buildCouchbaseConnection(uris, "default", "");
  }
}
