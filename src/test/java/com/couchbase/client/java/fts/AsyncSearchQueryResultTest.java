/*
 * Copyright (c) 2016 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.couchbase.client.java.fts;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

import java.util.List;

import com.couchbase.client.core.CouchbaseException;
import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.client.java.fts.result.AsyncSearchQueryResult;
import com.couchbase.client.java.fts.result.SearchMetrics;
import com.couchbase.client.java.fts.result.SearchQueryRow;
import com.couchbase.client.java.fts.result.facets.FacetResult;
import com.couchbase.client.java.fts.result.impl.DefaultAsyncSearchQueryResult;
import org.junit.Ignore;
import org.junit.Test;
import rx.observables.BlockingObservable;

public class AsyncSearchQueryResultTest {

    @Test
    @Ignore
    public void testJsonConversion() {
        DefaultAsyncSearchQueryResult.fromJson(JsonObject.empty());
        //TODO get several payload examples, assert them
    }

    @Test
    public void testHttp400Conversion() {
        AsyncSearchQueryResult result = DefaultAsyncSearchQueryResult.fromHttp400("some error message");

        assertThat(result).isNotNull();
        assertThat(result.status()).isNotNull();
        assertThat(result.status().errorCount()).isEqualTo(1);
        assertThat(result.status().totalCount()).isEqualTo(1);
        assertThat(result.status().successCount()).isEqualTo(0);

        List<FacetResult> facets = result.facets().toList()
                .toBlocking().singleOrDefault(null);
        assertThat(facets)
                .isNotNull()
                .isEmpty();

        SearchMetrics metrics = result.metrics().toBlocking().singleOrDefault(null);
        assertThat(metrics).isNotNull();
        assertThat(metrics.maxScore()).isEqualTo(0d);
        assertThat(metrics.took()).isEqualTo(0);
        assertThat(metrics.totalHits()).isEqualTo(0);

        BlockingObservable<SearchQueryRow> hits = result.hits().toBlocking();
        try {
            hits.single();
            fail("expected exception while getting hits with HTTP 400");
        } catch (Throwable t) {
            assertThat(t)
                    .isInstanceOf(CouchbaseException.class)
                    .hasMessageEndingWith("some error message");
        }
    }
}
