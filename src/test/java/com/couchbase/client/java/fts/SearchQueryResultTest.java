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

import static com.googlecode.catchexception.CatchException.catchException;
import static com.googlecode.catchexception.CatchException.caughtException;
import static com.googlecode.catchexception.CatchException.verifyException;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

import com.couchbase.client.core.CouchbaseException;
import com.couchbase.client.java.fts.result.AsyncSearchQueryResult;
import com.couchbase.client.java.fts.result.SearchQueryResult;
import com.couchbase.client.java.fts.result.SearchQueryRow;
import com.couchbase.client.java.fts.result.impl.DefaultAsyncSearchQueryResult;
import com.couchbase.client.java.fts.result.impl.DefaultSearchQueryResult;
import org.junit.Test;
import org.mockito.Mockito;
import rx.Observable;
import rx.exceptions.CompositeException;

public class SearchQueryResultTest {


    private static SearchQueryResult toSync(AsyncSearchQueryResult asyncResult) {
        return DefaultSearchQueryResult.FROM_ASYNC.call(asyncResult).toBlocking().singleOrDefault(null);
    }

    //TODO also test various 200 payloads

    @Test
    public void testHttp400Conversion() {
        SearchQueryResult result = toSync(DefaultAsyncSearchQueryResult.fromHttp400("some error message"));

        assertThat(result).isNotNull();
        assertThat(result.status()).isNotNull();
        assertThat(result.status().errorCount()).isEqualTo(1);
        assertThat(result.status().totalCount()).isEqualTo(1);
        assertThat(result.status().successCount()).isEqualTo(0);
        assertThat(result.facets())
                .isNotNull()
                .isEmpty();
        assertThat(result.metrics()).isNotNull();
        assertThat(result.metrics().maxScore()).isEqualTo(0d);
        assertThat(result.metrics().took()).isEqualTo(0);
        assertThat(result.metrics().totalHits()).isEqualTo(0);

        assertThat(result.hits()).isEmpty();
        assertThat(result.errors())
                .hasSize(1)
                .contains("some error message");
        catchException(result).hitsOrFail();
        assertThat(caughtException())
                .isInstanceOf(CouchbaseException.class)
                .hasMessage("some error message");
    }

    @Test
    public void testCompositeExceptionGivesMultipleErrors() {
        CompositeException e = new CompositeException(new RuntimeException("A"), new RuntimeException("B"));
        AsyncSearchQueryResult asyncResult = Mockito.spy(DefaultAsyncSearchQueryResult.fromHttp400("some error message"));
        when(asyncResult.hits()).thenReturn(Observable.<SearchQueryRow>error(e));
        SearchQueryResult result = toSync(asyncResult);

        assertThat(result.hits()).isEmpty();
        assertThat(result.errors()).hasSize(2);
        verifyException(result, CompositeException.class).hitsOrFail();
    }
}
