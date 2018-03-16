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

package com.couchbase.client;

import javax.naming.NamingEnumeration;
import javax.naming.directory.Attributes;
import javax.naming.directory.DirContext;
import javax.naming.directory.InitialDirContext;
import java.net.URI;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

/**
 * Utility class to generate seed nodes from various sources.
 */
public class BootstrapNodeUtil {

  /**
   * Contains the DNS environment.
   */
  private static Hashtable<String, String> DNS_ENV =
    new Hashtable<String, String>();

  /**
   * The default DNS factory to use.
   */
  private static final String DEFAULT_DNS_FACTORY =
    "com.sun.jndi.dns.DnsContextFactory";

  /**
   * The default DNS provider to use.
   */
  private static final String DEFAULT_DNS_PROVIDER = "dns:";

  static {
    DNS_ENV.put("java.naming.factory.initial", DEFAULT_DNS_FACTORY);
    DNS_ENV.put("java.naming.provider.url", DEFAULT_DNS_PROVIDER);
  }

  /**
   * Locate bootstrap nodes from a DNS SRV record.
   *
   * @param service the SRV identifier.
   * @return a list of boostrap URIs.
   */
  public static List<URI> locateFromDNS(String service) {
    List<URI> uris = new ArrayList<URI>();

    try {
      DirContext ctx = new InitialDirContext(DNS_ENV);
      Set<DnsRecord> sortedRecords = loadDnsRecords(service, ctx);
      for(DnsRecord record : sortedRecords) {
        uris.add(new URI("http://" + record.getHost() + ":" + record.getPort()
          + "/pools"));
      }
    } catch(Exception ex) {
      throw new RuntimeException("Could not locate URIs from DNS SRV.", ex);
    }

    return uris;
  }

  static Set<DnsRecord> loadDnsRecords(String service, DirContext ctx)
    throws Exception {
    Attributes attrs = ctx.getAttributes(service, new String[] { "SRV" });
    NamingEnumeration<?> servers = attrs.get("srv").getAll();
    Set<DnsRecord> sortedRecords = new TreeSet<DnsRecord>();
    while (servers.hasMore()) {
      DnsRecord record = DnsRecord.fromString((String) servers.next());
      sortedRecords.add(record);
    }
    return sortedRecords;
  }

  static class DnsRecord implements Comparable<DnsRecord> {
    private final int priority;
    private final int weight;
    private final int port;
    private final String host;

    public DnsRecord(int priority, int weight, int port, String host) {
      this.priority = priority;
      this.weight = weight;
      this.port = port;
      this.host = host.replaceAll("\\.$", "");
    }

    public int getPriority() {
      return priority;
    }

    public int getWeight() {
      return weight;
    }

    public int getPort() {
      return port;
    }

    public String getHost() {
      return host;
    }

    public static DnsRecord fromString(String input) {
      String[] splitted = input.split(" ");
      return new DnsRecord(
        Integer.parseInt(splitted[0]),
        Integer.parseInt(splitted[1]),
        Integer.parseInt(splitted[2]),
        splitted[3]
      );
    }

    @Override
    public String toString() {
      return "DnsRecord{" +
        "priority=" + priority +
        ", weight=" + weight +
        ", port=" + port +
        ", host='" + host + '\'' +
        '}';
    }

    @Override
    public int compareTo(DnsRecord o) {
      if (getPriority() < o.getPriority()) {
        return -1;
      } else {
        return 1;
      }
    }

  }

}
