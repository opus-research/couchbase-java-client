package com.couchbase.client.vbucket.cccp;


import com.couchbase.client.vbucket.ConfigurationProvider;
import com.couchbase.client.vbucket.config.Bucket;
import org.junit.Test;

import java.net.URI;
import java.util.Arrays;

public class AdvancedConfigurationProviderTest {

  @Test
  public void shouldFetchConfigOverBinary() throws Exception {
    ConfigurationProvider provider = new AdvancedConfigurationProvider(
      Arrays.asList(new URI("http://192.168.56.101:8091/pools")),
      "default",
      ""
    );

    Bucket bucket = provider.getBucketConfiguration("default");
    System.out.println(bucket.getConfig().getServers());
  }

  @Test
  public void shouldFetchConfigOverBinaryWithPassword() throws Exception {
    ConfigurationProvider provider = new AdvancedConfigurationProvider(
      Arrays.asList(new URI("http://192.168.56.101:8091/pools")),
      "pw",
      "pw"
    );

    Bucket bucket = provider.getBucketConfiguration("default");
    System.out.println(bucket.getConfig().getServers());
  }

  @Test
  public void shouldFetchConfigOverBinaryWithOneWrongNode() throws Exception {
    ConfigurationProvider provider = new AdvancedConfigurationProvider(
      Arrays.asList(new URI("http://unknown:8091/pools"), new URI("http://192.168.56.101:8091/pools")),
      "default",
      ""
    );

    Bucket bucket = provider.getBucketConfiguration("default");
    System.out.println(bucket.getConfig().getServers());
  }

  @Test
  public void shouldFallbackIfUnsupported() throws Exception {
    ConfigurationProvider provider = new AdvancedConfigurationProvider(
      Arrays.asList(new URI("http://localhost:8091/pools")),
      "default",
      ""
    );

    Bucket bucket = provider.getBucketConfiguration("default");
    System.out.println(bucket.getConfig().getServers());
  }

}
