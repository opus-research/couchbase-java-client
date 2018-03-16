package com.couchbase.client.vbucket.provider;

import com.couchbase.client.CouchbaseClient;
import com.couchbase.client.CouchbaseConnectionFactory;
import com.couchbase.client.vbucket.ConfigurationException;
import com.couchbase.client.vbucket.config.Bucket;
import org.junit.Test;

import java.net.URI;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;


public class BucketConfigurationProviderTest {
  
  @Test
  public void shouldBootstrapBothBinaryAndHttp() throws Exception {

    List<URI> seedNodes = Arrays.asList(new URI("http://127.0.0.1:8091/pools"));
    String bucket = "beer-sample";
    String password = "";

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
    List<URI> seedNodes = Arrays.asList(new URI("http://127.0.0.1:8091/pools"));
    String bucket = "default";
    String password = "";

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
    List<URI> seedNodes = Arrays.asList(new URI("http://127.0.0.1:8091/pools"));
    String bucket = "beer-sample";
    String password = "";

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
  public void shouldBootstrap() throws Exception {
    CouchbaseClient c = new CouchbaseClient(
      Arrays.asList(new URI("http://127.0.0.1:8091/pools")),
      "default",
      ""
    );

    c.set("foo", "bar").get();
    System.out.println(c.get("foo"));
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
