package com.couchbase.client.vbucket.provider;

import com.couchbase.client.CouchbaseClient;
import com.couchbase.client.CouchbaseConnectionFactory;
import com.couchbase.client.vbucket.ConfigurationException;
import com.couchbase.client.vbucket.config.Bucket;
import net.spy.memcached.TestConfig;
import org.junit.Before;
import org.junit.Test;

import java.net.URI;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.logging.ConsoleHandler;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.Logger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;


public class BucketConfigurationProviderTest {

  private List<URI> seedNodes;
  private String bucket;
  private String password;
  private String seedNode;

  @Before
  public void setup() throws Exception {
    //seedNode = TestConfig.IPV4_ADDR;
    seedNode = "192.168.56.101";

    seedNodes = Arrays.asList(new URI("http://" + seedNode
     + ":8091/pools"));
    bucket = "default";
    password = "";
  }

  @Test
  public void shouldBootstrapBothBinaryAndHttp() throws Exception {
    BucketConfigurationProvider provider = new BucketConfigurationProvider(
      seedNodes,
      bucket,
      password,
      new CouchbaseConnectionFactory(seedNodes, bucket, password)
    );

    assertTrue(provider.bootstrapBinary());
    Bucket binaryConfig = provider.getConfig();

    assertTrue(provider.bootstrapHttp());
    Bucket httpConfig = provider.getConfig();

    assertEquals(binaryConfig.getConfig().getServersCount(),
      httpConfig.getConfig().getServersCount());
  }

  @Test(expected = ConfigurationException.class)
  public void shouldThrowExceptionIfNoConfigFound() throws Exception {
    BucketConfigurationProvider provider = new FailingBucketConfigurationProvider(
      seedNodes,
      bucket,
      password,
      new CouchbaseConnectionFactory(seedNodes, bucket, password),
      true,
      true
    );

    provider.bootstrap();
  }

  @Test
  public void shouldReloadHttpConfigOnSignalOutdated() throws Exception {
    List<URI> seedNodes = Arrays.asList(
      new URI("http://foobar:8091/pools"),
      new URI("http://" + seedNode + ":8091/pools")
    );

    BucketConfigurationProvider provider = new FailingBucketConfigurationProvider(
      seedNodes,
      bucket,
      password,
      new CouchbaseConnectionFactory(seedNodes, bucket, password),
      true,
      false
    );

    provider.bootstrap();
    provider.signalOutdated();
  }

  @Test
  public void shouldReloadBinaryConfigOnSignalOutdated() throws Exception {
    List<URI> seedNodes = Arrays.asList(
      new URI("http://foobar:8091/pools"),
      new URI("http://" + seedNode + ":8091/pools")
    );

    BucketConfigurationProvider provider = new FailingBucketConfigurationProvider(
      seedNodes,
      bucket,
      password,
      new CouchbaseConnectionFactory(seedNodes, bucket, password),
      false,
      true
    );

    provider.bootstrap();
    provider.signalOutdated();
  }

  @Test
  public void shouldBootstrapFromCouchbaseClient() throws Exception {
    CouchbaseClient c = new CouchbaseClient(seedNodes, bucket, password);

    assertTrue(c.set("foo", "bar").get());
    assertEquals("bar", c.get("foo"));
  }

  public void shouldIgnoreInvalidNodeOnBootstrap() throws Exception {
    BucketConfigurationProvider provider = new FailingBucketConfigurationProvider(
      seedNodes,
      bucket,
      password,
      new CouchbaseConnectionFactory(seedNodes, bucket, password),
      false,
      false
    );

    provider.bootstrap();
  }

  @Test
  public void testFoo() throws Exception {

    Properties systemProperties = System.getProperties();
    systemProperties.put("net.spy.log.LoggerImpl", "net.spy.memcached.compat.log.SunLogger");
    System.setProperties(systemProperties);

    Logger logger = Logger.getLogger("com.couchbase.client.vbucket");
    logger.setLevel(Level.FINEST);
    for(Handler h : logger.getParent().getHandlers()) {
      if(h instanceof ConsoleHandler){
        h.setLevel(Level.FINEST);
      }
    }

    CouchbaseClient c = new CouchbaseClient(Arrays.asList(new URI("http://192.168.56.101:8091/pools")), bucket, password);

    int i = 0;
    while(true) {
      if (i >= 1000) {
        i = 0;
      }
      try {
        c.set("foo" + (++i), "bar").get();
        c.get("foo" + i);
      } catch(Exception ex) {
        ex.printStackTrace();
      }
    }
    //Thread.sleep(TimeUnit.DAYS.toMillis(1));
  }

  /**
   * A provider that can fail either one or both of the bootstrap mechanisms.
   */
  static class FailingBucketConfigurationProvider
    extends BucketConfigurationProvider {

    private final boolean failBinary;
    private final boolean failHttp;
    FailingBucketConfigurationProvider(List<URI> seedNodes, String bucket,
      String password, CouchbaseConnectionFactory connectionFactory,
      boolean failBinary, boolean failHttp) {
      super(seedNodes, bucket, password, connectionFactory);
      this.failBinary = failBinary;
      this.failHttp = failHttp;
    }

    @Override
    boolean bootstrapBinary() {
      if (failBinary) {
        return false;
      } else {
        return super.bootstrapBinary();
      }
    }

    @Override
    boolean bootstrapHttp() {
      if (failHttp) {
        return false;
      } else {
        return super.bootstrapHttp();
      }
    }
  }



}
