/*
 * Copyright (c) 2014 Couchbase, Inc.
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
package com.couchbase.client.java.util.features;

import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.cluster.ClusterInfo;
import com.couchbase.client.java.document.json.JsonArray;
import com.couchbase.client.java.document.json.JsonObject;

/**
 * An helper class that can be used to check if
 * a particular {@link CouchbaseFeature} is available on a given {@link Cluster}.
 *
 * @author Simon Baslé
 * @since 2.1.0
 */
public class FeaturesHelper {

    /**
     * Checks the availability of a specified {@link CouchbaseFeature} on a given {@link Cluster}
     * via its {@link ClusterInfo}.
     *
     * @param info the cluster's info
     * @param feature the feature to check for
     * @return true if minimum node server version is compatible with the feature, false otherwise
     */
    public static boolean checkAvailable(ClusterInfo info, CouchbaseFeature feature) {
        Version minVersion = getMinVersion(info);
        return feature.isAvailableOn(minVersion);
    }

    /**
     * Return the smallest node version in the cluster from which the {@link ClusterInfo} was taken.
     *
     * @param info the cluster's info
     * @return the smallest server version in the cluster
     */
    public static Version getMinVersion(ClusterInfo info) {
        Version minVersion = Version.VERY_BIG;
        try {
            JsonObject raw = info.raw();
            if (!raw.containsKey("nodes")) {
                return Version.NO_VERSION;
            }
            JsonArray nodes = raw.getArray("nodes");
            for (int i = 0; i < nodes.size(); i++) {
                JsonObject node = nodes.getObject(i);
                if (node.containsKey("version")) {
                    String versionFull = node.getString("version");
                    Version version = Version.parseVersion(versionFull);
                    if (version.compareTo(minVersion) < 0) {
                            minVersion = version;
                    }
                }
            }
            return minVersion;
        } catch (Exception e) {
            return Version.NO_VERSION;
        }
    }
}
