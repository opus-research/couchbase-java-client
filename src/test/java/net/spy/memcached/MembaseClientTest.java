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

package net.spy.memcached;

import java.net.URI;
import java.util.Arrays;

/**
 * A MembaseClientTest.
 */
public class MembaseClientTest extends BinaryClientTest {
  @Override
  protected void initClient() throws Exception {
    initClient(new MembaseConnectionFactory(Arrays.asList(URI.create("http://"
        + TestConfig.IPV4_ADDR + ":8091/pools")), "default", ""));
  }

  @Override
  protected String getExpectedVersionSource() {
    if (TestConfig.IPV4_ADDR.equals("127.0.0.1")) {
      return "/127.0.0.1:11210";
    }
    return TestConfig.IPV4_ADDR + ":11210";
  }

  @Override
  protected void initClient(ConnectionFactory cf) throws Exception {
    client = new MembaseClient((MembaseConnectionFactory) cf);
  }

  @Override
  public void testAvailableServers() {
    // MembaseClient tracks hostname and ip address of servers need to
    // make sure the available server list is 2 * (num servers)
    try {
      Thread.sleep(10); // Let the client warm up
    } catch (InterruptedException e) {
      fail("Interrupted while client was warming up");
    }
    assert client.getAvailableServers().size() == 1 : "Num servers "
            + client.getAvailableServers().size();
  }

  public void testNumVBuckets() throws Exception {
    // Check for power of two values
    if (TestConfig.isMembase()) {
      int num = ((MembaseClient)client).getNumVBuckets();

      assertTrue("NumVBuckets has to be a power of two", (num & -num)== num);
    }
  }

  public void testGATTimeout() throws Exception {
    assertNull(client.get("gatkey"));
    assert client.set("gatkey", 1, "gatvalue").get().booleanValue();
    assert client.getAndTouch("gatkey", 2).getValue().equals("gatvalue");
    Thread.sleep(1300);
    assert client.get("gatkey").equals("gatvalue");
    Thread.sleep(2000);
    assertNull(client.getAndTouch("gatkey", 3));
  }

  public void testTouchTimeout() throws Exception {
    assertNull(client.get("touchkey"));
    assert client.set("touchkey", 1, "touchvalue").get().booleanValue();
    assert client.touch("touchkey", 2).get().booleanValue();
    Thread.sleep(1300);
    assert client.get("touchkey").equals("touchvalue");
    Thread.sleep(2000);
    assertFalse(client.touch("touchkey", 3).get().booleanValue());
  }

  public void testSimpleGetl() throws Exception {
    assertNull(client.get("getltest"));
    client.set("getltest", 0, "value");
    ((MembaseClient)client).getAndLock("getltest", 3);
    Thread.sleep(2000);
    assert !client.set("getltest", 1, "newvalue").get().booleanValue()
      : "Key wasn't locked for the right amount of time";
    Thread.sleep(2000);
    assert client.set("getltest", 1, "newvalue").get().booleanValue()
      : "Key was locked for too long";
  }

  public void testGetStatsSlabs() throws Exception {
    // Empty
  }

  public void testGetStatsSizes() throws Exception {
    // Empty
  }

  public void testGetStatsCacheDump() throws Exception {
    // Empty
  }

  public void testStupidlyLargeSetAndSizeOverride() throws Exception {
    // Empty
  }

  protected void syncGetTimeoutsInitClient() throws Exception {
    initClient(new MembaseConnectionFactory(Arrays.asList(URI
        .create("http://localhost:8091/pools")), "default", "") {
      @Override
      public long getOperationTimeout() {
        return 2;
      }

      @Override
      public int getTimeoutExceptionThreshold() {
        return 1000000;
      }
    });
  }
}
