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
package com.couchbase.client.java.fts.result.impl;

import com.couchbase.client.java.fts.result.AsyncSearchQueryResult;
import com.couchbase.client.java.fts.result.SearchMetrics;
import com.couchbase.client.java.fts.result.SearchQueryRow;
import com.couchbase.client.java.fts.result.SearchStatus;
import com.couchbase.client.java.fts.result.facets.FacetResult;
import rx.Observable;

public class DefaultAsyncSearchQueryResult implements AsyncSearchQueryResult {

    private final SearchStatus status;
    private final Observable<SearchQueryRow> hits;
    private final Observable<FacetResult> facets;
    private final Observable<SearchMetrics> metrics;

    public DefaultAsyncSearchQueryResult(SearchStatus status,
            Observable<SearchQueryRow> hits, Observable<FacetResult> facets,
            Observable<SearchMetrics> metrics) {
        this.status = status;
        this.hits = hits;
        this.facets = facets;
        this.metrics = metrics;
    }

    @Override
    public SearchStatus status() {
        return status;
    }

    @Override
    public Observable<SearchQueryRow> hits() {
        return hits;
    }

    @Override
    public Observable<FacetResult> facets() {
        return facets;
    }

    @Override
    public Observable<SearchMetrics> metrics() {
        return metrics;
    }
}
