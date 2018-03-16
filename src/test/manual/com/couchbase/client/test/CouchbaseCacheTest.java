/**
 * Copyright (C) 2006-2009 Dustin Sallings
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

package com.couchbase.client.test;

import com.couchbase.springframework.CouchbaseCache;
import com.couchbase.springframework.CouchbaseCacheManager;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;

/**
 * Verify the Couchbase Cache for multiple Caches mapped
 * to multiple buckets.
 *
 * Essentially, this test sets a few different Caches.
 *
 * This test expects a cluster running
 * Couchbase server with the names of the buckets as specified
 * in the program.
 *
 * Change the IP address of the nodes below before.
 *
 * CouchbaseCacheTest server_address
 */

public class CouchbaseCacheTest {

    public static void main(String[] args) throws
            URISyntaxException, IOException {
      
        CouchbaseCacheManager cacheManager = new CouchbaseCacheManager();

        List<CouchbaseCache> cacheList = new ArrayList<CouchbaseCache>();

        List baseURIs = new ArrayList<URI>();

        if (args.length != 1) {
            System.err.println("usage: server_address");
            System.exit(1);
        }

        baseURIs.add(new URI(String.format("http://%s:8091/pools",
                args[0])));

        CouchbaseCache colorsCache = new
                CouchbaseCache("colorsCache",
                baseURIs, "Rags0R", "");
        colorsCache.put("RED", "Red");
        colorsCache.put("GREEN", "Green");
        colorsCache.put("BLUE", "Blue");

        cacheList.add(colorsCache);

        CouchbaseCache statesCache = new
                CouchbaseCache("statesCache",
                baseURIs, "Rags1R", "");
        statesCache.put("MA", "MA");
        statesCache.put("NH", "NH");
        statesCache.put("ME", "ME");

        cacheList.add(statesCache);

        CouchbaseCache numCache = new
                CouchbaseCache("numCache",
                baseURIs, "Rags2R", "");
        numCache.put("evenNumbers", new
                HashSet<Integer>(Arrays.asList(0, 2, 4, 6, 8)));
        numCache.put("oddNumbers", new
                HashSet<Integer>(Arrays.asList(1, 3, 5, 7, 9)));
        numCache.put("fibonacciNumbers", new
                HashSet<Integer>(Arrays.asList(1, 1, 2, 3, 5, 8, 13)));             
        cacheList.add(numCache);          

        CouchbaseCache arrayCache = new CouchbaseCache("arrayCache",
                baseURIs, "Rags3R", "");
        Integer[] integerArray = new Integer[3];
        integerArray[0] = 2;
        integerArray[1] = 3;
        integerArray[2] = 4;
        arrayCache.put("intArray", integerArray);
        cacheList.add(arrayCache);

        Collection<CouchbaseCache> cacheCollection = cacheList;
        cacheManager.setCaches(cacheCollection);
        cacheManager.afterPropertiesSet();

        printCacheNames(cacheManager);
        printCache(cacheManager);
        System.out.println("Evicting fibonacciNumbers and printing");
        cacheManager.getCache("numCache").evict("fibonacciNumbers");
        printCache(cacheManager);
        System.out.println("clearing numCache and printing");
        cacheManager.getCache("numCache").clear();
        printCache(cacheManager);
        System.exit(0);
    }

    private static void printCacheNames(CouchbaseCacheManager cacheManager) {
        for (String cacheName : cacheManager.getCacheNames()) {
            System.out.println("Cache name is " + cacheName);
        }
    }

    private static void printCache(CouchbaseCacheManager cacheManager) {
        System.out.println("Value of 'MA' from 'statesCache' is "
            + cacheManager.getCache("statesCache").get("MA").get());
        System.out.println("Value of 'NH' from 'statesCache' is "
            + cacheManager.getCache("statesCache").get("NH").get());
        System.out.println("Value of 'XX' from 'statesCache' is "
            + cacheManager.getCache("statesCache").get("XX").get());

        System.out.println("Value of 'oddNumbers' "
            + "from 'numCache' is "
            + cacheManager.getCache("numCache")
            .get("oddNumbers").get());

        System.out.println("Value of 'fibonacciNumbers' "
            + "from 'numCache' is "
            + cacheManager.getCache("numCache")
            .get("fibonacciNumbers").get());

        Integer[] intArray;
        intArray = (Integer []) 
                cacheManager.getCache("arrayCache").get("intArray").get();
        System.out.println("Value of 'arrays' from 'arrayCache' is "
            + intArray[0] + " " +  intArray[1] + " " + intArray[2]);
    }
}
