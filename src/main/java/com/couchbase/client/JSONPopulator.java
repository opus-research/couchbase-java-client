package com.couchbase.client;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.URI;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.UUID;

import net.spy.memcached.BinaryConnectionFactory;
import net.spy.memcached.MemcachedClient;
import net.spy.memcached.internal.OperationFuture;

public class JSONPopulator {

  public static void main(String args[]) throws Exception{
    List<URI> uris = new LinkedList<URI>();
    uris.add(URI.create("http://localhost:9000/pools"));
    //CouchbaseClient client = new CouchbaseClient(uris, "default", "");
    MemcachedClient client = new MemcachedClient(new BinaryConnectionFactory(),
        Arrays.asList(new InetSocketAddress("localhost", 12001)));
    del(client);
    client.shutdown();
  }

  public static void stats(MemcachedClient client, String stat) throws Exception {
    Map<SocketAddress, Map<String, String>> stats = client.getStats(stat);

    for (Entry<SocketAddress, Map<String, String>> sStats: stats.entrySet()) {
      for (Entry<String, String> iStat: sStats.getValue().entrySet()) {
        System.out.println(iStat.getKey() + ": " + iStat.getValue());
      }
    }
  }

  public static void del(MemcachedClient client) throws Exception {
    for (int i = 0; i < 100000; i++) {
      if (!client.delete("key" + i).get().booleanValue()) {
        System.err.println("Error deleteing data for key" + i);
      }
    }
  }

  public static void set(MemcachedClient client) throws Exception {
    for (int i = 0; i < 100000; i++) {
      OperationFuture<Boolean> op = client.set("key" + i, 0, generateDocument(5));
      if (!op.get().booleanValue()) {
        System.err.println("Error setting data for key" + i);
      }
    }
  }

  public static String generateDocument(int fields) {
    StringBuilder sb = new StringBuilder();
    sb.append("{");

    for (int i = 0; i < fields; i++) {
      sb.append(generateField("field" + i, UUID.randomUUID().toString()));
      if (i < fields - 1)
      sb.append(",");
    }
    sb.append("}");
    return sb.toString();
  }

  public static String generateField(String key, String value) {
    return "\"" + key + "\":\"" + value + "\"";
  }

  public static void randomSetDel(MemcachedClient client, int setPercent, int iterations)
  throws Exception {
  int sets = 0;
  int deletes = 0;
  int misses = 0;
  List<String> setKeys = new LinkedList<String>();

  for (int i = 0; i < iterations; i++) {
    int rand = (int) (Math.random() * 100);
    if (rand < setPercent) {
      String key = "key" + i;
      if (!client.set("key" + i, 0, generateDocument(5)).get().booleanValue()) {
        System.err.println("Error setting data for key" + i);
        misses++;
      } else {
        setKeys.add(key);
        sets++;
      }
    } else if (setKeys.size() > 0) {
      String key = setKeys.get(0);
      if (!client.delete(key).get().booleanValue()) {
        System.err.println("Error deleteing data for key" + i);
        misses++;
      } else {
        setKeys.remove(key);
        deletes++;
      }
    } else {
      misses++;
    }
  }
  System.out.println("Sets: " + sets);
  System.out.println("Deletes: " + deletes);
  System.out.println("Misses: " + misses);

  while(setKeys.size() > 0) {
    String key = setKeys.get(0);
    OperationFuture<Boolean> op = client.delete(key);
    if (!op.get().booleanValue()) {
      System.err.println("Error deleteing data for " + key + ": " + op.getStatus().getMessage());
      setKeys.add(key);
    } else {
      setKeys.remove(0);
    }
  }
}
}
