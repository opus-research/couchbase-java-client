/**
 * Copyright (C) 2015 Couchbase, Inc.
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

package com.couchbase.client.java.search;

import com.couchbase.client.java.document.json.JsonObject;

/**
 * @author Sergey Avseyev
 */
public class DCPSourceParams implements SourceParams {
    public static final String AUTH_USER = "default";
    public static final String AUTH_PASSWORD = "";
    public static final String AUTH_SASL_USER = "";
    public static final String AUTH_SASL_PASSWORD = "";
    public static final float CLUSTER_MANAGER_BACKOFF_FACTOR = 0;
    public static final int CLUSTER_MANAGER_SLEEP_INIT_MS = 0;
    public static final int CLUSTER_MANAGER_SLEEP_MAX_MS = 20000;
    public static final float DATA_MANAGER_BACKOFF_FACTOR = 0;
    public static final int DATA_MANAGER_SLEEP_INIT_MS = 0;
    public static final int DATA_MANAGER_SLEEP_MAX_MS = 20000;
    public static final int FEED_BUFFER_SIZE_BYTES = 0;
    public static final float FEED_BUFFER_ACK_THRESHOLD = 0;

    private final String authUser;
    private final String authPassword;
    private final String authSaslUser;
    private final String authSaslPassword;
    private final float clusterManagerBackoffFactor;
    private final int clusterManagerSleepInitMS;
    private final int clusterManagerSleepMaxMS;
    private final float dataManagerBackoffFactor;
    private final int dataManagerSleepInitMS;
    private final int dataManagerSleepMaxMS;
    private final int feedBufferSizeBytes;
    private final float feedBufferAckThreshold;

    protected DCPSourceParams(final Builder builder) {
        authUser = builder.authUser;
        authPassword = builder.authPassword;
        authSaslUser = builder.authSaslUser;
        authSaslPassword = builder.authSaslPassword;
        clusterManagerBackoffFactor = builder.clusterManagerBackoffFactor;
        clusterManagerSleepInitMS = builder.clusterManagerSleepInitMS;
        clusterManagerSleepMaxMS = builder.clusterManagerSleepMaxMS;
        dataManagerBackoffFactor = builder.dataManagerBackoffFactor;
        dataManagerSleepInitMS = builder.dataManagerSleepInitMS;
        dataManagerSleepMaxMS = builder.dataManagerSleepMaxMS;
        feedBufferSizeBytes = builder.feedBufferSizeBytes;
        feedBufferAckThreshold = builder.feedBufferAckThreshold;
    }

    public String authUser() {
        return authUser;
    }

    public String authPassword() {
        return authPassword;
    }

    public String authSaslUser() {
        return authSaslUser;
    }

    public String authSaslPassword() {
        return authSaslPassword;
    }

    // Factor (like 1.5) to increase sleep time between retries
    // in connecting to a cluster manager node.
    public float clusterManagerBackoffFactor() {
        return clusterManagerBackoffFactor;
    }

    // Initial sleep time (milliseconds) before first retry to cluster manager.
    public int clusterManagerSleepInitMS() {
        return clusterManagerSleepInitMS;
    }

    // Maximum sleep time (milliseconds) between retries to cluster manager.
    public int clusterManagerSleepMaxMS() {
        return clusterManagerSleepMaxMS;
    }

    // Factor (like 1.5) to increase sleep time between retries
    // in connecting to a data manager node.
    public float dataManagerBackoffFactor() {
        return dataManagerBackoffFactor;
    }

    // Initial sleep time (milliseconds) before first retry to data manager.
    public int dataManagerSleepInitMS() {
        return dataManagerSleepInitMS;
    }

    // Maximum sleep time (milliseconds) between retries to data manager.
    public int dataManagerSleepMaxMS() {
        return dataManagerSleepMaxMS;
    }

    // Buffer size in bytes provided for DCP flow control.
    public int feedBufferSizeBytes() {
        return feedBufferSizeBytes;
    }

    // Used for DCP flow control and buffer-ack messages when this
    // percentage of FeedBufferSizeBytes is reached.
    public float feedBufferAckThreshold() {
        return feedBufferAckThreshold;
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public JsonObject json() {
        JsonObject json = JsonObject.create();
        json.put("authUser", authUser);
        json.put("authPassword", authPassword);
        json.put("authSaslUser", authSaslUser);
        json.put("authSaslPassword", authSaslPassword);
        json.put("clusterManagerBackoffFactor", clusterManagerBackoffFactor);
        json.put("clusterManagerSleepInitMS", clusterManagerSleepInitMS);
        json.put("clusterManagerSleepMaxMS", clusterManagerSleepMaxMS);
        json.put("dataManagerBackoffFactor", dataManagerBackoffFactor);
        json.put("dataManagerSleepInitMS", dataManagerSleepInitMS);
        json.put("dataManagerSleepMaxMS", dataManagerSleepMaxMS);
        json.put("feedBufferSizeBytes", feedBufferSizeBytes);
        json.put("feedBufferAckThreshold", feedBufferAckThreshold);
        return json;
    }

    public static class Builder {
        public String authUser = AUTH_USER;
        public String authPassword = AUTH_PASSWORD;
        public String authSaslUser = AUTH_SASL_USER;
        public String authSaslPassword = AUTH_SASL_PASSWORD;
        public float clusterManagerBackoffFactor = CLUSTER_MANAGER_BACKOFF_FACTOR;
        public int clusterManagerSleepInitMS = CLUSTER_MANAGER_SLEEP_INIT_MS;
        public int clusterManagerSleepMaxMS = CLUSTER_MANAGER_SLEEP_MAX_MS;
        public float dataManagerBackoffFactor = DATA_MANAGER_BACKOFF_FACTOR;
        public int dataManagerSleepInitMS = DATA_MANAGER_SLEEP_INIT_MS;
        public int dataManagerSleepMaxMS = DATA_MANAGER_SLEEP_MAX_MS;
        public int feedBufferSizeBytes = FEED_BUFFER_SIZE_BYTES;
        public float feedBufferAckThreshold = FEED_BUFFER_ACK_THRESHOLD;

        protected Builder() {
        }

        public DCPSourceParams build() {
            return new DCPSourceParams(this);
        }

        public Builder authUser(final String authUser) {
            this.authUser = authUser;
            return this;
        }

        public Builder authPassword(final String authPassword) {
            this.authPassword = authPassword;
            return this;
        }

        public Builder authSaslUser(final String authSaslUser) {
            this.authSaslUser = authSaslUser;
            return this;
        }

        public Builder authSaslPassword(final String authSaslPassword) {
            this.authSaslPassword = authSaslPassword;
            return this;
        }

        public Builder clusterManagerBackoffFactor(final float clusterManagerBackoffFactor) {
            this.clusterManagerBackoffFactor = clusterManagerBackoffFactor;
            return this;
        }

        public Builder clusterManagerSleepInitMS(final int clusterManagerSleepInitMS) {
            this.clusterManagerSleepInitMS = clusterManagerSleepInitMS;
            return this;
        }

        public Builder clusterManagerSleepMaxMS(final int clusterManagerSleepMaxMS) {
            this.clusterManagerSleepMaxMS = clusterManagerSleepMaxMS;
            return this;
        }

        public Builder dataManagerBackoffFactor(final float dataManagerBackoffFactor) {
            this.dataManagerBackoffFactor = dataManagerBackoffFactor;
            return this;
        }

        public Builder dataManagerSleepInitMS(final int dataManagerSleepInitMS) {
            this.dataManagerSleepInitMS = dataManagerSleepInitMS;
            return this;
        }

        public Builder dataManagerSleepMaxMS(final int dataManagerSleepMaxMS) {
            this.dataManagerSleepMaxMS = dataManagerSleepMaxMS;
            return this;
        }

        public Builder feedBufferSizeBytes(final int feedBufferSizeBytes) {
            this.feedBufferSizeBytes = feedBufferSizeBytes;
            return this;
        }

        public Builder feedBufferAckThreshold(final float feedBufferAckThreshold) {
            this.feedBufferAckThreshold = feedBufferAckThreshold;
            return this;
        }
    }
}