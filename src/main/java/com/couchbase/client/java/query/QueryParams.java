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
package com.couchbase.client.java.query;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import com.couchbase.client.java.document.json.JsonArray;
import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.client.java.document.json.JsonValue;
import com.couchbase.client.java.query.consistency.ScanConsistency;

/**
 * Parameter Object for {@link Query queries} that allows to fluently set most of the N1QL query parameters:
 *  - server side timeout
 *  - credentials
 *  - client context ID
 *  - scan consistency (with associated scan vector and/or scan wait if relevant)
 *
 * Note that these are different from statement-related named parameters or positional parameters.
 *
 * @author Simon Baslé
 * @since 2.1
 */
public class QueryParams {

    private String serverSideTimeout;
    private ScanConsistency consistency;
    private JsonValue scanVector;
    private String scanWait;
    private JsonArray creds;
    private String clientContextId;

    private QueryParams() { }

    /**
     * Modifies the given N1QL query (as a {@link JsonObject}) to reflect these {@link QueryParams}.
     * @param queryJson the N1QL query
     */
    public void injectParams(JsonObject queryJson) {
        if (this.serverSideTimeout != null) {
            queryJson.put("timeout", this.serverSideTimeout);
        }
        if (this.consistency == ScanConsistency.AT_PLUS && this.scanVector != null) {
                queryJson.put("scan_consistency", this.consistency.n1ql());
                queryJson.put("scan_vector", this.scanVector);
        } else if (this.consistency != null) {
            queryJson.put("scan_consistency", this.consistency.n1ql());
        }
        if (this.scanWait != null
                && (ScanConsistency.AT_PLUS == this.consistency
                || ScanConsistency.REQUEST_PLUS == this.consistency
                || ScanConsistency.STATEMENT_PLUS == this.consistency)) {
            queryJson.put("scan_wait", this.scanWait);
        }
        if (this.creds != null && !this.creds.isEmpty()) {
            queryJson.put("creds", this.creds);
        }
        if (this.clientContextId != null) {
            queryJson.put("client_context_id", this.clientContextId);
        }
    }
    
    private static String durationToN1qlFormat(long duration, TimeUnit unit) {
        switch (unit) {
            case NANOSECONDS:
                return duration + "ns";
            case MICROSECONDS:
                return duration + "us";
            case MILLISECONDS:
                return duration + "ms";
            case SECONDS:
                return duration + "s";
            case MINUTES:
                return duration + "m";
            case HOURS:
                return duration + "h";
            case DAYS:
            default:
                return unit.toHours(duration) + "h";
        }
    }

    /**
     * Start building a {@link QueryParams}, allowing to customize an N1QL request.
     *
     * @return a new {@link QueryParams}
     */
    public static QueryParams build() {
        return new QueryParams();
    }

    /**
     * Sets a maximum timeout for processing on the server side.
     *
     * @param timeout the duration of the timeout.
     * @param unit the unit of the timeout, from nanoseconds to hours.
     * @return this {@link QueryParams} for chaining.
     */
    public QueryParams serverSideTimeout(long timeout, TimeUnit unit) {
        this.serverSideTimeout = durationToN1qlFormat(timeout, unit);
        return this;
    }

    /**
     * Adds a bucket authentication credential to the list of credentials for the request.
     *
     * @param bucket the authenticated bucket name.
     * @param password the password for the bucket.
     * @return this {@link QueryParams} for chaining.
     */
    public QueryParams addCredential(String bucket, String password) {
        if (this.creds == null) {
            this.creds = JsonArray.empty();
        }
        this.creds.add(JsonObject.create()
            .put("user", "local:" + bucket)
            .put("pass", password));
        return this;
    }

    /** Adds an admin authentication credential to the list of credentials for the request.
     *
     * @param adminName the login of the administrator.
     * @param password the password for the administrator.
     * @return this {@link QueryParams} for chaining.
     */
    public QueryParams addAdminCredential(String adminName, String password) {
        if (this.creds == null) {
            this.creds = JsonArray.empty();
        }
        this.creds.add(JsonObject.create()
                                 .put("user", "admin:" + adminName)
                                 .put("pass", password));
        return this;
    }

    /**
     * Adds a client context ID to the request, that will be sent back in the response, allowing clients
     * to meaningfully trace requests/responses when many are exchanged.
     *
     * @param clientContextId the client context ID (null to send none)
     * @return this {@link QueryParams} for chaining.
     */
    public QueryParams withContextId(String clientContextId) {
        this.clientContextId = clientContextId;
        return this;
    }

    /**
     * Sets scan consistency to AT_PLUS, using a full scan vector.
     *
     * This implements bounded consistency. The request includes a scan_vector parameter and value,
     * which is used as a lower bound. This can be used to implement read-your-own-writes (RYOW).
     *
     * @param scanVector an array of 1024 sequence numbers/timestamps, one for each vBucket.
     * @return this {@link QueryParams} for chaining.
     * @throws IllegalArgumentException if the scanVector is not full (length != 1024).
     */
    public QueryParams consistencyAtPlus(int[] scanVector) {
        if (scanVector.length != 1024) {
            throw new IllegalArgumentException("Full Scan Vector must contain seqno for all 1024 vbuckets");
        }
        this.consistency = ScanConsistency.AT_PLUS;
        List<Integer> fullVector = new ArrayList<Integer>(scanVector.length);
        for (int i : scanVector) {
            fullVector.add(i);
        }
        this.scanVector = JsonArray.from(fullVector);
        return this;
    }

    /**
     * Sets scan consistency to AT_PLUS, using a sparse scan vector.
     *
     * This implements bounded consistency. The request includes a scan_vector parameter and value,
     * which is used as a lower bound. This can be used to implement read-your-own-writes (RYOW).
     *
     * @param sparseScanVector A {@link Map} giving the sequence number/timestamp for each vBucket number (String)
     * @return this {@link QueryParams} for chaining.
     */
    public QueryParams consistencyAtPlus(Map<String, Integer> sparseScanVector) {
        this.consistency = ScanConsistency.AT_PLUS;
        this.scanVector = JsonObject.from(sparseScanVector);
        return this;
    }

    /**
     * Sets scan consistency to NOT_BOUNDED and unsets the {@link #scanWait} if it was set.
     *
     * This is the default (for single-statement requests).
     * No timestamp vector is used in the index scan.
     * This is also the fastest mode, because we avoid the cost of obtaining the vector,
     * and we also avoid any wait for the index to catch up to the vector.
     *
     * @return this {@link QueryParams} for chaining.
     */
    public QueryParams consistencyNotBounded() {
        this.consistency = ScanConsistency.NOT_BOUNDED;
        this.scanVector = null;
        this.scanWait = null;
        return this;
    }

    /**
     * Sets scan consistency to REQUEST_PLUS.
     *
     * This implements strong consistency per request.
     * Before processing the request, a current vector is obtained.
     * The vector is used as a lower bound for the statements in the request.
     * If there are DML statements in the request, RYOW is also applied within the request.
     *
     * @return this {@link QueryParams} for chaining.
     */
    public QueryParams consistencyRequestPlus() {
        this.consistency = ScanConsistency.REQUEST_PLUS;
        this.scanVector = null;
        return this;
    }

    /**
     * Sets scan consistency to STATEMENT_PLUS.
     *
     * This implements strong consistency per statement.
     * Before processing each statement, a current vector is obtained
     * and used as a lower bound for that statement.
     *
     * @return this {@link QueryParams} for chaining.
     */
    public QueryParams consistencyStatementPlus() {
        this.consistency = ScanConsistency.STATEMENT_PLUS;
        this.scanVector = null;
        return this;
    }

    /**
     * If the {@link #consistencyNotBounded() NOT_BOUNDED scan consistency} has been chosen, does nothing.
     *
     * Otherwise, sets the maximum time the client is willing to wait for an index to catch up to the
     * vector timestamp in the request.
     *
     * @param wait the duration.
     * @param unit the unit for the duration.
     * @return this {@link QueryParams} for chaining.
     */
    public QueryParams scanWait(long wait, TimeUnit unit) {
        if (this.consistency == ScanConsistency.NOT_BOUNDED) {
            this.scanWait = null;
        } else {
            this.scanWait = durationToN1qlFormat(wait, unit);
        }
        return this;
    }
}
