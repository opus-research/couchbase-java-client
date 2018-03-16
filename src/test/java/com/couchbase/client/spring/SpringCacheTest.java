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
package com.couchbase.client.spring;


import com.couchbase.spring.CouchbaseCache;
import com.couchbase.spring.CouchbaseCacheManager;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;


import net.spy.memcached.TestConfig;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * A SpringCacheTest.
 */
public class SpringCacheTest {
  private static final String SERVER = TestConfig.IPV4_ADDR;
  private CouchbaseCacheManager cbCacheManager;
  private List<CouchbaseCache> cacheList;
  /*
   * SpringCacheTest.
   */
  public SpringCacheTest() {
  }

  @BeforeClass
  public static void setUpClass() {
  }

  @AfterClass
  public static void tearDownClass() {
  }

  @Before
  public void setUp() throws IOException, URISyntaxException {
    cbCacheManager = new CouchbaseCacheManager();

    cacheList = new ArrayList<CouchbaseCache>();

    ArrayList<URI> serverList = new ArrayList<URI>();
    serverList.add(new URI(String.format("http://%s:8091/pools", SERVER)));
    CouchbaseCache colorsCache = new
            CouchbaseCache("colorsCache",
            serverList, "default", "");
    cacheList.add(colorsCache);

    cbCacheManager.setCaches(cacheList);
    cbCacheManager.afterPropertiesSet();
  }

  @After
  public void tearDown() {
  }

  @Test
  public void testgetCaches() throws URISyntaxException, IOException {
    assertEquals(cbCacheManager.getCacheNames().size(), 1);
    assertTrue(cbCacheManager.getCacheNames().contains("colorsCache"));
  }

  @Test
  public void testPut() throws URISyntaxException, IOException {
    CouchbaseCache cbCache = cbCacheManager.getCache("colorsCache");
    assertNotNull(cbCache);
    cbCache.put("RED", "red");
    cbCache.put("GREEN", "green");
    cbCache.put("BLUE", "blue");
    assertEquals(cbCache.get("RED").get(), "red");
    assertNull((cbCache.get("YELLOW").get()));
  }

  @Test
  public void testEvict() throws URISyntaxException, IOException {
    CouchbaseCache cbCache = cbCacheManager.getCache("colorsCache");
    assertNotNull(cbCache);
    cbCache.evict("RED");
    assertNull((cbCache.get("RED").get()));
  }

  @Test
  public void testClear() throws URISyntaxException, IOException {
    CouchbaseCache cbCache = cbCacheManager.getCache("colorsCache");
    assertNotNull(cbCache);
    cbCache.clear();
    assertNull((cbCache.get("GREEN").get()));
  }
}
