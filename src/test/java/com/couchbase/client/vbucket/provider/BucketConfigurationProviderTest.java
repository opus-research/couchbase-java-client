package com.couchbase.client.vbucket.provider;

import com.couchbase.client.CouchbaseConnectionFactory;
import com.couchbase.client.vbucket.config.Bucket;
import org.junit.Test;

import java.net.URI;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Created by michaelnitschinger on 17/01/14.
 */
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


    //assertTrue(provider.bootstrapHttp());
    //Bucket httpConfig = provider.getConfig();

    //assertEquals(binaryConfig.getConfig().getServersCount(),
    //  httpConfig.getConfig().getServersCount());
  }

}
