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

import com.couchbase.client.vbucket.config.Bucket;

import java.net.URI;
import java.util.List;

/**
 * A ConfigurationProvider.
 */
public interface ConfigurationProvider {

  /**
   * Connects to the REST service and retrieves the bucket configuration from
   * the first pool available.
   *
   * @param bucketname bucketname
   * @return vbucket configuration
   * @throws ConfigurationException
   */
  Bucket getBucketConfiguration(String bucketname);

  /**
   * Update the bucket including configuration.
   *
   * @param bucketname the name of the bucket
   * @param newBucket the new bucket including config
   */
  void updateBucket(String bucketname, Bucket newBucket);

  /**
   * Subscribes for configuration updates.
   *
   * @param bucketName bucket name to receive configuration for
   * @param rec reconfigurable that will receive updates
   * @throws ConfigurationException
   */
  void subscribe(String bucketName, Reconfigurable rec);

  void markForResubscribe(String bucketName, Reconfigurable rec);

  /**
   * Unsubscribe from updates on a given bucket and given reconfigurable.
   *
   * @param vbucketName bucket name
   * @param rec reconfigurable
   */
  void unsubscribe(String vbucketName, Reconfigurable rec);

  /**
   * Shutdowns a monitor connections to the REST service.
   */
  void shutdown();

  /**
   * Retrieves a default bucket name i.e. 'default'.
   *
   * @return the anonymous bucket's name i.e. 'default'
   */
  String getAnonymousAuthBucket();

  void finishResubscribe();

  /**
   * Returns the current Reconfigurable object.
   */
  Reconfigurable getReconfigurable();

  /**
   * Returns the current bucket name.
   */
  String getBucket();

  /**
   * Update the configured baseList on bootstrap with a new one.
   *
   * @param baseList the list to replace with.
   */
  void updateBaseListFromConfig(List<URI> baseList);

}
