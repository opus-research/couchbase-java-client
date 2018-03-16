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

import org.junit.Test;

import javax.naming.directory.BasicAttribute;
import javax.naming.directory.BasicAttributes;
import javax.naming.directory.DirContext;

import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class BootstrapNodeUtilTest {

  /**
   * Tests DNS loading from a mocked source.
   *
   * If testing against a real DNS SRV serving host is needed,
   * "_xmpp-server._tcp.gmail.com" can be used experimentally against the
   * {@link BootstrapNodeUtil#locateFromDNS(String)} method..
   */
  @Test
  public void shouldLoadDnsRecords() throws Exception {
    String service = "_seeds._tcp.couchbase.com";

    BasicAttributes basicAttributes = new BasicAttributes(true);
    BasicAttribute basicAttribute = new BasicAttribute("SRV");
    basicAttribute.add("20 0 8091 node2.couchbase.com.");
    basicAttribute.add("10 0 8091 node1.couchbase.com.");
    basicAttribute.add("30 0 8091 node3.couchbase.com.");
    basicAttribute.add("40 0 8091 node4.couchbase.com.");
    basicAttributes.put(basicAttribute);

    DirContext mockedContext = mock(DirContext.class);
    when(mockedContext.getAttributes(service, new String[] { "SRV" }))
      .thenReturn(basicAttributes);


    Set<BootstrapNodeUtil.DnsRecord> records =
      BootstrapNodeUtil.loadDnsRecords(service, mockedContext);

    int nodeNumber = 1;
    for (BootstrapNodeUtil.DnsRecord record : records) {
      assertEquals("node" + nodeNumber + ".couchbase.com", record.getHost());
      assertEquals(nodeNumber * 10, record.getPriority());
      assertEquals(8091, record.getPort());
      nodeNumber++;
    }
  }

}
