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

import com.couchbase.client.CouchbaseClient;
import com.couchbase.client.CouchbaseConnectionFactory;
import com.couchbase.client.CouchbaseConnectionFactoryBuilder;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import net.spy.memcached.TestConfig;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.springframework.beans.factory.BeanFactory;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import static org.junit.Assert.*;

/**
 * A SpringBaseTest.
 */
public class SpringBaseTest {
  private static final String SERVER_URI = "http://" + TestConfig.IPV4_ADDR
      + ":8091/pools";
  private static URL url;
  public SpringBaseTest() {
  }

  @BeforeClass
  public static void setUpClass() throws IOException, URISyntaxException {
    // Create the application context file
    url = ClassLoader.getSystemResource("cbGenerate.xml");
    PrintWriter pw = new
            PrintWriter(new FileWriter(new File(url.toURI().getPath())));
    pw.println("<?xml version=\"1.0\" encoding=\"UTF-8\"?>");
    pw.println("<beans xmlns=\"http://www.springframework.org/schema/beans\"");
    pw.println("       xmlns:xsi=\"http://www.w3.org/2001/"
            + "XMLSchema-instance\"");
    pw.println("       xsi:schemaLocation=\"http://www.springframework.org/"
            + "schema/beans");
    pw.println("       http://www.springframework.org/"
            + "schema/beans/spring-beans-2.5.xsd\">");
    pw.println("");
    pw.println("  <!-- Configuration for Couchbase configuration beans -->");
    pw.println("");
    pw.println("  <bean name=\"couchbaseClient\" "
            + "class=\"com.couchbase.client.CouchbaseClient\">");
    pw.println("  <!-- CouchbaseClient three arguments. "
            + "URL list, bucket name and bucket password. -->");
    pw.println("            <constructor-arg>");
    pw.println("                <list>");
    pw.println("                    <bean id=\"url1\" "
            + "class=\"java.net.URI\" >");
    pw.println("                            <constructor-arg>");
    pw.println("                                <value>" + SERVER_URI
            + "</value>");
    pw.println("                            </constructor-arg>");
    pw.println("                    </bean>");
    pw.println("                </list>");
    pw.println("            </constructor-arg>");
    pw.println("");
    pw.println("            <constructor-arg value=\"default\"/>");
    pw.println("");
    pw.println("            <constructor-arg value=\"\"/>");
    pw.println("");
    pw.println("  </bean>");
    pw.println("");
    pw.println("</beans>");
    //Close the output stream
    pw.close();

    url = ClassLoader.getSystemResource("cfbGenerate.xml");

    pw = new PrintWriter(new FileWriter(new File(url.toURI().getPath())));

    System.err.print(url);
    pw.println("<?xml version=\"1.0\" encoding=\"UTF-8\"?>");
    pw.println("<beans xmlns=\"http://www.springframework.org/schema/beans\"");
    pw.println("       xmlns:xsi=\"http://www.w3.org/"
            + "2001/XMLSchema-instance\"");
    pw.println("       xsi:schemaLocation=\"http://www.springframework.org/"
            + "schema/beans");
    pw.println("       http://www.springframework.org/schema/"
            + "beans/spring-beans-2.5.xsd\">");
    pw.println("");
    pw.println("  <!-- Configuration for Couchbase configuration beans -->");
    pw.println("");
    pw.println("  <bean name=\"couchbasecfb\" "
            + "class=\"com.couchbase.client."
            + "CouchbaseConnectionFactoryBuilder\">");
    pw.println("  </bean>");
    pw.println("");
    pw.println("</beans>");
    //Close the output stream
    pw.close();
  }

  @AfterClass
  public static void tearDownClass() throws URISyntaxException {
    url = ClassLoader.getSystemResource("cbGenerate.xml");
    File file = new File(url.toURI().getPath());
    file.delete();
    url = ClassLoader.getSystemResource("cfbGenerate.xml");
    file = new File(url.toURI().getPath());
    file.delete();
  }

  @Before
  public void setUp() {
  }

  @After
  public void tearDown() {
  }

  @Test
  public void testCouchbaseBean() {
    final String key = "SpringCouchbase" + System.currentTimeMillis();
    final String value = "Spring/Couchbase";
    BeanFactory beanFactory =
      new ClassPathXmlApplicationContext("cbGenerate.xml");
    CouchbaseClient c =
            (CouchbaseClient) beanFactory.getBean("couchbaseClient");
    c.set(key, 0, value);
    assertEquals(value, c.get(key));
    c.delete(key);
    c.shutdown(3, TimeUnit.SECONDS);
  }

  @Test
  public void testCouchbaseConnectionFactoryBuilderBean() throws
      URISyntaxException, IOException {
    final String key = "SpringcfbCouchbase" + System.currentTimeMillis();
    final String value = "Spring/Couchbase";
    BeanFactory beanFactory =
      new ClassPathXmlApplicationContext("cfbGenerate.xml");
    CouchbaseConnectionFactoryBuilder cfb =
        (CouchbaseConnectionFactoryBuilder)
        beanFactory.getBean("couchbasecfb");
    URI base = new URI(String.format("%s", SERVER_URI));
    List<URI> baseURIs = new ArrayList<URI>();
    baseURIs.add(base);
    CouchbaseConnectionFactory cf =
        cfb.buildCouchbaseConnection(baseURIs, "default", "", "");
    CouchbaseClient c = new CouchbaseClient(cf);
    c.set(key, 0, value);
    assertEquals(value, c.get(key));
    c.delete(key);
    c.shutdown(3, TimeUnit.SECONDS);
  }
}
