/**
 * Copyright (C) 2009-2013 Couchbase, Inc.
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

package com.couchbase.client.vbucket;

import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.couchbase.client.vbucket.config.Bucket;
import com.couchbase.client.vbucket.config.CacheConfig;
import com.couchbase.client.vbucket.config.Node;

/**
 * Implements a stub configuration provider for testing.
 */
public class ConfigurationProviderMock
  implements ConfigurationProvider {

  @Override
  public Bucket getBucketConfiguration(String bucketname) {

	  CacheConfig config = new CacheConfig(1);
	  config.setServers(Arrays.asList("badurl"+":8091"));
	  URI streamingURI = URI.create("http://"+"badurl"+":8091");
	  List<Node> nodes = new ArrayList<Node>();

	  return new Bucket(bucketname, config, streamingURI, nodes);
  }

  @Override
  public void subscribe(String bucketName, Reconfigurable rec) {}

  @Override
  public void markForResubscribe(String bucketName, Reconfigurable rec) {}

  @Override
  public void unsubscribe(String vbucketName, Reconfigurable rec) {}

  @Override
  public void shutdown() {}

  @Override
  public String getAnonymousAuthBucket() {
    return "";
  }

  @Override
  public void finishResubscribe() {}

  @Override
  public Reconfigurable getReconfigurable() {
	return new ReconfigurableMock();
  }

  @Override
  public String getBucket() {
    return "resubscriber-bucket";
  }
}
