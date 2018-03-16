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
package com.couchbase.client.java.fts.result.facets;

public interface FacetResult {

    String name();
    String field();

    /**
     * @return the total number of *valued* facet results. Total = {@link #other()} + terms (but doesn't include
     * {@link #missing()}).
     */
    long total();

    /**
     * @return the number of results that couldn't be faceted, missing the adequate value. Not matter how many more
     * buckets are added to the original facet, these result won't ever be included in one.
     */
    long missing();

    /**
     * @return the number of results that could have been faceted (because they have a value for the facet's field) but
     * weren't, due to not having a bucket in which they belong. Adding a bucket can result in these results being faceted.
     */
    long other();
}
