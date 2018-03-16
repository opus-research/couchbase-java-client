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

package com.couchbase.client;

import com.couchbase.client.vbucket.config.Config;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URL;
import java.util.Collection;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import net.spy.memcached.ConnectionFactoryBuilder;
import net.spy.memcached.ConnectionObserver;
import net.spy.memcached.FailureMode;
import net.spy.memcached.HashAlgorithm;
import net.spy.memcached.OperationFactory;
import net.spy.memcached.compat.CloseUtil;
import net.spy.memcached.ops.Operation;
import net.spy.memcached.transcoders.Transcoder;

/**
 * CouchbaseConnectionFactoryBuilder.
 *
 */

public class CouchbaseConnectionFactoryBuilder extends ConnectionFactoryBuilder{

  private static final String MODE_PRODUCTION = "production";
  private static final String MODE_DEVELOPMENT = "development";
  private static final String DEV_PREFIX = "dev_";
  private static final String PROD_PREFIX = "";
  private static String modePrefix = PROD_PREFIX;
  private static String modeMessage = "";

  private static String obsPollIntervalProp;
  private static String obsPollMaxProp;
  private static String shouldOptimizeProp;
  private static String opTimeoutProp;
  private static String timeoutExceptionThresholdProp;
  private static String maxReconnectDelayProp;
  private static String readBufSizeProp;
  private static String isDaemonProp;
  private static String opQueueMaxBlockTimeProp;
  private static String reconnThresholdTimeProp;

  private long obsPollInterval;
  private int obsPollMax;
  private long reconnThresholdTimeMsecs =
    CouchbaseConnectionFactory.DEFAULT_MIN_RECONNECT_INTERVAL;
  /**
   * Properties priority from highest to lowest:
   *
   * 1. Property defined in user code.
   * 2. Property defined on command line.
   * 3. Property defined in cbclient.properties.
   */
  static {
    Properties properties = new Properties();

    FileInputStream fs = null;
    try {
      URL url = ClassLoader.getSystemResource("cbclient.properties");
      if (url != null) {
        fs = new FileInputStream(new File(url.getFile()));
        properties.load(fs);
      }
    } catch (IOException e) {
      // Properties file doesn't exist. Error logged later.
    } finally {
      if (fs != null) {
        CloseUtil.close(fs);
      }
    }
    // Merge the command-line properties
    properties.putAll(System.getProperties());

    String viewmode = properties.getProperty("viewmode", MODE_PRODUCTION);
    if (viewmode == null) {
      modeMessage = "viewmode property isn't defined. Setting viewmode to"
        + " production mode";
      modePrefix = PROD_PREFIX;
    } else if (viewmode.equals(MODE_PRODUCTION)) {
      modeMessage = "viewmode set to production mode";
      modePrefix = PROD_PREFIX;
    } else if (viewmode.equals(MODE_DEVELOPMENT)) {
      modeMessage = "viewmode set to development mode";
      modePrefix = DEV_PREFIX;
    } else {
      modeMessage = "unknown value \"" + viewmode + "\" for property viewmode"
          + " Setting to production mode";
      modePrefix = PROD_PREFIX;
    }

    obsPollIntervalProp = properties.getProperty("obsPollInterval");
    obsPollMaxProp = properties.getProperty("obsPollMax");
    shouldOptimizeProp = properties.getProperty("shouldOptimize");
    opTimeoutProp = properties.getProperty("opTimeout");
    timeoutExceptionThresholdProp =
            properties.getProperty("timeoutExceptionThreshold");
    maxReconnectDelayProp = properties.getProperty("maxReconnectDelay");
    readBufSizeProp = properties.getProperty("readBufSize");
    isDaemonProp = properties.getProperty("isDaemon");
    opQueueMaxBlockTimeProp = properties.getProperty("opQueueMaxBlockTime");
    reconnThresholdTimeProp = properties.getProperty("reconnThresholdTime");
  }

  public CouchbaseConnectionFactoryBuilder() {
    super();
    if (obsPollIntervalProp != null) {
      obsPollInterval = Long.parseLong(obsPollIntervalProp);
    }
    if (obsPollMaxProp != null) {
      obsPollMax = Integer.parseInt(obsPollMaxProp);
    }
    if (opTimeoutProp != null) {
      opTimeout = Long.parseLong(opTimeoutProp);
    }
    if (shouldOptimizeProp != null) {
      shouldOptimize = Boolean.parseBoolean(shouldOptimizeProp);
    }
    if (timeoutExceptionThresholdProp != null) {
      timeoutExceptionThreshold =
              Integer.parseInt(timeoutExceptionThresholdProp);
    }
    if (maxReconnectDelayProp != null) {
      maxReconnectDelay = Integer.parseInt(maxReconnectDelayProp);
    }
    if (readBufSizeProp != null) {
      readBufSize = Integer.parseInt(readBufSizeProp);
    }
    if (isDaemonProp != null) {
      isDaemon = Boolean.parseBoolean(isDaemonProp);
    }
    if (opQueueMaxBlockTimeProp != null) {
      opQueueMaxBlockTime = Integer.parseInt(opQueueMaxBlockTimeProp);
    }
    if (reconnThresholdTimeProp != null) {
      reconnThresholdTimeMsecs = Integer.parseInt(reconnThresholdTimeProp);
    }
  }

  private Config vBucketConfig;

  public Config getVBucketConfig() {
    return vBucketConfig;
  }

  public void setVBucketConfig(Config config) {
    this.vBucketConfig = config;
  }

  public void setReconnectThresholdTime(long time, TimeUnit unit) {
    reconnThresholdTimeMsecs = TimeUnit.MILLISECONDS.convert(time, unit);
  }

  public CouchbaseConnectionFactoryBuilder setObsPollInterval(long interval) {
    obsPollInterval = interval;
    return this;
  }

  public CouchbaseConnectionFactoryBuilder setObsPollMax(int maxPoll) {
    obsPollMax = maxPoll;
    return this;
  }

  /**
   * Get the CouchbaseConnectionFactory set up with the provided parameters.
   * Note that a CouchbaseConnectionFactory requires the failure mode is set
   * to retry, and the locator type is discovered dynamically based on the
   * cluster you are connecting to. As a result, these values will be
   * overridden upon calling this function.
   *
   * @param baseList a list of URI's that will be used to connect to the cluster
   * @param bucketName the name of the bucket to connect to, also used for
   * username
   * @param pwd the password for the bucket
   * @return a CouchbaseConnectionFactory object
   * @throws IOException
   */
  public CouchbaseConnectionFactory buildCouchbaseConnection(
      final List<URI> baseList, final String bucketName, final String pwd)
    throws IOException {
    return this.buildCouchbaseConnection(baseList, bucketName, bucketName, pwd);
  }


  /**
   * Get the CouchbaseConnectionFactory set up with the provided parameters.
   * Note that a CouchbaseConnectionFactory requires the failure mode is set
   * to retry, and the locator type is discovered dynamically based on the
   * cluster you are connecting to. As a result, these values will be
   * overridden upon calling this function.
   *
   * @param baseList a list of URI's that will be used to connect to the cluster
   * @param bucketName the name of the bucket to connect to
   * @param usr the username for the bucket
   * @param pwd the password for the bucket
   * @return a CouchbaseConnectionFactory object
   * @throws IOException
   */
  public CouchbaseConnectionFactory buildCouchbaseConnection(
      final List<URI> baseList, final String bucketName, final String usr,
      final String pwd) throws IOException {
    return new CouchbaseConnectionFactory(baseList, bucketName, pwd) {

      @Override
      public BlockingQueue<Operation> createOperationQueue() {
        return opQueueFactory == null ? super.createOperationQueue()
            : opQueueFactory.create();
      }

      @Override
      public BlockingQueue<Operation> createReadOperationQueue() {
        return readQueueFactory == null ? super.createReadOperationQueue()
            : readQueueFactory.create();
      }

      @Override
      public BlockingQueue<Operation> createWriteOperationQueue() {
        return writeQueueFactory == null ? super.createReadOperationQueue()
            : writeQueueFactory.create();
      }

      @Override
      public Transcoder<Object> getDefaultTranscoder() {
        return transcoder == null ? super.getDefaultTranscoder() : transcoder;
      }

      @Override
      public FailureMode getFailureMode() {
        return failureMode;
      }

      @Override
      public HashAlgorithm getHashAlg() {
        return hashAlg;
      }

      @Override
      public Collection<ConnectionObserver> getInitialObservers() {
        return initialObservers;
      }

      @Override
      public OperationFactory getOperationFactory() {
        return opFact == null ? super.getOperationFactory() : opFact;
      }

      @Override
      public long getOperationTimeout() {
        return opTimeout == -1 ? super.getOperationTimeout() : opTimeout;
      }

      @Override
      public int getReadBufSize() {
        return readBufSize == -1 ? super.getReadBufSize() : readBufSize;
      }

      @Override
      public boolean isDaemon() {
        return isDaemon;
      }

      @Override
      public boolean shouldOptimize() {
        return false;
      }

      @Override
      public boolean useNagleAlgorithm() {
        return useNagle;
      }

      @Override
      public long getMaxReconnectDelay() {
        return maxReconnectDelay;
      }

      @Override
      public long getOpQueueMaxBlockTime() {
        return opQueueMaxBlockTime > -1 ? opQueueMaxBlockTime
            : super.getOpQueueMaxBlockTime();
      }

      @Override
      public int getTimeoutExceptionThreshold() {
        return timeoutExceptionThreshold;
      }

      public long getMinReconnectInterval() {
        return reconnThresholdTimeMsecs;
      }

      @Override
      public long getObsPollInterval() {
        return obsPollInterval;
      }

      @Override
      public int getObsPollMax() {
        return obsPollMax;
      }

      @Override
      public String getViewModePrefix() {
        return modePrefix;
      }

      @Override
      public String getViewModeMessage() {
        return modeMessage;
      }

    };
  }

  /**
   * @return the obsPollInterval
   */
  public long getObsPollInterval() {
    return obsPollInterval;
  }

  /**
   * @return the obsPollMax
   */
  public int getObsPollMax() {
    return obsPollMax;
  }
}
