package com.couchbase.client.vbucket.provider;

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
      new CouchbaseConnectionFactory(seedNodes, bucket, password)
    );

    provider.bootstrap();
  }

  /**
   * This provider doesn't even try to load the configs, but returns false
   * (indicating failure) immediately.
   */
  static class FailingBucketConfigurationProvider
    extends BucketConfigurationProvider {

    FailingBucketConfigurationProvider(List<URI> seedNodes, String bucket,
      String password, CouchbaseConnectionFactory connectionFactory) {
      super(seedNodes, bucket, password, connectionFactory);
    }

    @Override
    boolean bootstrapBinary() {
      return false;
    }

    @Override
    boolean bootstrapHttp() {
      return false;
    }
  }



}
