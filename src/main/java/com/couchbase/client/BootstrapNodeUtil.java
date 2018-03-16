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
 * The {@link BootstrapNodeUtil} provides various helper methods to generate
 * bootstrap URIs from different input formats.
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
   * The DNS SRV records need to be configured on a reachable DNS server. An
   * example configuration could look like the following:
   *
   * <pre>
   *  _cbnodes._tcp.example.com.  0  IN  SRV  20  0  389  node2.example.com.
   *  _cbnodes._tcp.example.com.  0  IN  SRV  10  0  389  node1.example.com.
   *  _cbnodes._tcp.example.com.  0  IN  SRV  30  0  389  node3.example.com.
   * </pre>
   *
   * Now if "_cbnodes._tcp.example.com" is passed in as the argument, the three
   * nodes configured will be parsed and put in the returned URI list. Note that
   * the priority is respected (in this example, node1 will be the first one
   * in the list, followed by node2 and node3). As of now, weighting is not
   * supported.
   *
   * @param service the DNS SRV service name.
   * @return a list of ordered boostrap URIs by their weight.
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

  /**
   * Helper method to load DNS records from the given context.
   *
   * @param service the service name.
   * @param ctx the context.
   * @return returns a sorted set of found records.
   * @throws Exception if something goes wrong during the load.
   */
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

  /**
   * A typesafe representation of a DNS record.
   */
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
