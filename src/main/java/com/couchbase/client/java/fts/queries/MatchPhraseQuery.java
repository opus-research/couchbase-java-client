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
package com.couchbase.client.java.fts.queries;

import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.client.java.fts.SearchParams;
import com.couchbase.client.java.fts.SearchQuery;

public class MatchPhraseQuery extends SearchQuery {

    private final String matchPhrase;
    private String field;
    private String analyzer;

    public MatchPhraseQuery(String matchPhrase, SearchParams searchParams) {
        super(searchParams);
        this.matchPhrase = matchPhrase;
    }

    @Override
    public MatchPhraseQuery boost(double boost) {
        super.boost(boost);
        return this;
    }

    public MatchPhraseQuery field(String field) {
        this.field = field;
        return this;
    }

    public MatchPhraseQuery analyzer(String analyzer) {
        this.analyzer = analyzer;
        return this;
    }

    @Override
    protected void injectParams(JsonObject input) {
        super.injectParams(input);

        input.put("match_phrase", matchPhrase);
        if (field != null) {
            input.put("field", field);
        }
        if (analyzer != null) {
            input.put("analyzer", analyzer);
        }
    }
}
