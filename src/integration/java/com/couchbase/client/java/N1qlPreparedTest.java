/*
 * Copyright (C) 2016 Couchbase, Inc.
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
package com.couchbase.client.java;

import static org.junit.Assert.assertEquals;

import java.util.Collections;
import java.util.List;

import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.client.java.query.N1qlParams;
import com.couchbase.client.java.query.N1qlQuery;
import com.couchbase.client.java.query.N1qlQueryResult;
import com.couchbase.client.java.query.N1qlQueryRow;
import com.couchbase.client.java.query.consistency.ScanConsistency;
import com.couchbase.client.java.util.CouchbaseTestContext;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Integration tests of the N1QL Query features.
 *
 * @author Simon Baslé
 * @since 2.1
 */
public class N1qlPreparedTest {

    private static CouchbaseTestContext ctx;

    private static final ScanConsistency CONSISTENCY = ScanConsistency.REQUEST_PLUS;

    @BeforeClass
    public static void init() throws InterruptedException {
        ctx = CouchbaseTestContext.builder()
                .adhoc(true)
                .bucketQuota(100)
                .bucketName("queryPrepared")
                .build()
                .ignoreIfNoN1ql()
        .ensurePrimaryIndex();

        ctx.bucket().upsert(JsonDocument.create("test1", JsonObject.create().put("item", "value")));
        ctx.bucket().upsert(JsonDocument.create("test2", JsonObject.create().put("item", 123)));
    }

    @AfterClass
    public static void cleanup() {
        ctx.destroyBucketAndDisconnect();
    }

    @Test
    public void test() {
        N1qlQuery query = N1qlQuery.simple("SELECT * FROM `" + ctx.bucketName() + "`", N1qlParams.build().consistency(CONSISTENCY).adhoc(false));
        N1qlQueryResult result = ctx.bucket().query(query);
        List<N1qlQueryRow> list = result.allRows();
        List<JsonObject> errors = result.errors();
        assertEquals(Collections.emptyList(), errors);
        assertEquals(2, list.size());
        System.out.println("Prepare and execute: " + result.info().executionTime());

        for (int i = 0; i < 30; i++) {
            System.out.println("#" + i + ": " + ctx.bucket().query(query)
                .info().executionTime());
        }
    }
}
