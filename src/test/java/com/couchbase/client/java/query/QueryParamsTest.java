/*
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import com.couchbase.client.java.document.json.JsonArray;
import com.couchbase.client.java.document.json.JsonObject;
import org.junit.Test;

/**
 * Tests on {@link QueryParams}.
 *
 * @author Simon Baslé
 * @since 2.1
 */
public class QueryParamsTest {

    @Test
    public void shouldInjectCorrectConsistencies() {
        QueryParams p = QueryParams.build().consistencyNotBounded();

        JsonObject actual = JsonObject.empty();
        p.injectParams(actual);

        assertEquals("not_bounded", actual.get("scan_consistency"));

        p.consistencyRequestPlus();
        p.injectParams(actual);
        assertEquals(1, actual.size());
        assertEquals("request_plus", actual.getString("scan_consistency"));

        p.consistencyStatementPlus();
        p.injectParams(actual);
        assertEquals(1, actual.size());
        assertEquals("statement_plus", actual.getString("scan_consistency"));

        Map<String, Integer> emptyVector = Collections.emptyMap();
        p.consistencyAtPlus(emptyVector);
        p.injectParams(actual);
        assertEquals(2, actual.size());
        assertEquals("at_plus", actual.getString("scan_consistency"));
        assertNotNull(actual.get("scan_vector"));
    }

    @Test
    public void consistencyNotBoundedShouldEraseScanWaitAndVector() {
        QueryParams p = QueryParams.build()
            .scanWait(12, TimeUnit.SECONDS)
            .consistencyNotBounded();

        JsonObject expected = JsonObject.create()
            .put("scan_consistency", "not_bounded");
        JsonObject actual = JsonObject.empty();
        p.injectParams(actual);

        assertEquals(expected, actual);
    }

    @Test
    public void shouldIgnoreScanWaitIfConsistencyNotBounded() {
        QueryParams p = QueryParams.build()
           .consistencyNotBounded()
           .scanWait(12, TimeUnit.SECONDS);

        JsonObject expected = JsonObject.create()
            .put("scan_consistency", "not_bounded");
        JsonObject actual = JsonObject.empty();
        p.injectParams(actual);

        assertEquals(expected, actual);
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldRejectAtPlusWithBadFullVector() {
        QueryParams.build().consistencyAtPlus(new int[] { 1, 2, 3});
    }


    @Test
    public void shouldProduceSparseVectorWithAtPlusMap() {
        QueryParams p = QueryParams.build()
            .consistencyAtPlus(Collections.singletonMap("5", 123));

        JsonObject actual = JsonObject.empty();
        p.injectParams(actual);

        assertEquals("at_plus", actual.getString("scan_consistency"));
        assertTrue(actual.get("scan_vector") instanceof JsonObject);
        assertEquals(1, actual.getObject("scan_vector").size());
        assertEquals(123, actual.getObject("scan_vector").get("5"));
    }

    @Test
    public void shouldProduceFullVectorWithAtPlusArray() {
        int[] vector = new int[1024];
        Arrays.fill(vector, 22);
        QueryParams p = QueryParams.build().consistencyAtPlus(vector);

        JsonObject actual = JsonObject.empty();
        p.injectParams(actual);

        assertEquals("at_plus", actual.getString("scan_consistency"));
        assertTrue(actual.get("scan_vector") instanceof JsonArray);
        assertEquals(1024, actual.getArray("scan_vector").size());
        assertEquals(22, actual.getArray("scan_vector").get(13));
    }

    @Test
    public void shouldAllowScanWaitOnlyForCorrectConsistencies() {
        QueryParams p = QueryParams.build()
                                   .scanWait(12, TimeUnit.SECONDS)
                                   .consistencyAtPlus(Collections.singletonMap("5", 123));

        JsonObject actual = JsonObject.empty();
        p.injectParams(actual);
        assertEquals("12s", actual.getString("scan_wait"));

        p.consistencyRequestPlus();

        actual = JsonObject.empty();
        p.injectParams(actual);
        assertEquals("12s", actual.getString("scan_wait"));

        p.consistencyStatementPlus();

        actual = JsonObject.empty();
        p.injectParams(actual);
        assertEquals("12s", actual.getString("scan_wait"));

        p.consistencyNotBounded();
        actual = JsonObject.empty();
        assertFalse(actual.containsKey("scan_wait"));

    }

    @Test
    public void shouldInjectClientId() {
        QueryParams p = QueryParams.build()
                                   .withContextId("test");

        JsonObject expected = JsonObject.create()
                                        .put("client_context_id", "test");
        JsonObject actual = JsonObject.empty();
        p.injectParams(actual);

        assertEquals(expected, actual);
    }

    @Test
    public void shouldInjectTimeoutNanos() {
        QueryParams p = QueryParams.build().serverSideTimeout(24, TimeUnit.NANOSECONDS);

        JsonObject expected = JsonObject.create().put("timeout", "24ns");
        JsonObject actual = JsonObject.empty();
        p.injectParams(actual);

        assertEquals(expected, actual);
    }
    @Test
    public void shouldInjectTimeoutMicros() {
        QueryParams p = QueryParams.build().serverSideTimeout(24, TimeUnit.MICROSECONDS);

        JsonObject expected = JsonObject.create().put("timeout", "24us");
        JsonObject actual = JsonObject.empty();
        p.injectParams(actual);

        assertEquals(expected, actual);
    }
    @Test
    public void shouldInjectTimeoutMillis() {
        QueryParams p = QueryParams.build().serverSideTimeout(24, TimeUnit.MILLISECONDS);

        JsonObject expected = JsonObject.create().put("timeout", "24ms");
        JsonObject actual = JsonObject.empty();
        p.injectParams(actual);

        assertEquals(expected, actual);
    }
    @Test
    public void shouldInjectTimeoutSeconds() {
        QueryParams p = QueryParams.build().serverSideTimeout(24, TimeUnit.SECONDS);

        JsonObject expected = JsonObject.create().put("timeout", "24s");
        JsonObject actual = JsonObject.empty();
        p.injectParams(actual);

        assertEquals(expected, actual);
    }
    @Test
    public void shouldInjectTimeoutMinutes() {
        QueryParams p = QueryParams.build().serverSideTimeout(24, TimeUnit.MINUTES);

        JsonObject expected = JsonObject.create().put("timeout", "24m");
        JsonObject actual = JsonObject.empty();
        p.injectParams(actual);

        assertEquals(expected, actual);
    }

    @Test
    public void shouldInjectTimeoutHours() {
        QueryParams p = QueryParams.build().serverSideTimeout(24, TimeUnit.HOURS);

        JsonObject expected = JsonObject.create().put("timeout", "24h");
        JsonObject actual = JsonObject.empty();
        p.injectParams(actual);

        assertEquals(expected, actual);
    }

    @Test
    public void shouldInjectTimeoutHoursIfDays() {
        QueryParams p = QueryParams.build().serverSideTimeout(2, TimeUnit.DAYS);

        JsonObject expected = JsonObject.create().put("timeout", "48h");
        JsonObject actual = JsonObject.empty();
        p.injectParams(actual);

        assertEquals(expected, actual);
    }

    @Test
    public void shouldInjectLocalCredentialForBucket() {
        QueryParams p = QueryParams.build()
                                   .addCredential("bucket", "pwd");

        JsonObject expectedCred = JsonObject.create()
                .put("user", "local:bucket")
                .put("pass", "pwd");

        JsonObject actual = JsonObject.empty();
        p.injectParams(actual);

        assertTrue(actual.containsKey("creds"));
        assertFalse(actual.getArray("creds").isEmpty());
        assertEquals(expectedCred, actual.getArray("creds").get(0));
    }

    @Test
    public void shouldInjectAdminCredentialForAdmin() {
        QueryParams p = QueryParams.build()
                                   .addAdminCredential("john", "pwd");

        JsonObject expectedCred = JsonObject.create()
                                            .put("user", "admin:john")
                                            .put("pass", "pwd");

        JsonObject actual = JsonObject.empty();
        p.injectParams(actual);

        assertTrue(actual.containsKey("creds"));
        assertFalse(actual.getArray("creds").isEmpty());
        assertEquals(expectedCred, actual.getArray("creds").get(0));
    }

    @Test
    public void shouldAppendCredentials() {
        QueryParams p = QueryParams.build()
                .addCredential("bucket", "pwd")
                .addAdminCredential("john", "pwd")
                .addCredential("bucket2", "pwd2");

        JsonObject actual = JsonObject.empty();
        p.injectParams(actual);

        assertNotNull(actual.getArray("creds"));
        assertEquals(3, actual.getArray("creds").size());
    }

    @Test
    public void shouldDoNothingIfParamsEmpty() {
        QueryParams p = QueryParams.build();
        JsonObject empty = JsonObject.empty();
        p.injectParams(empty);

        assertTrue(empty.isEmpty());
    }
}
