/**
 * Copyright (C) 2015 Couchbase, Inc.
 * <p/>
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 * <p/>
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 * <p/>
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALING
 * IN THE SOFTWARE.
 */

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.bucket.BucketManager;
import com.couchbase.client.java.env.CouchbaseEnvironment;
import com.couchbase.client.java.env.DefaultCouchbaseEnvironment;
import com.couchbase.client.java.search.DCPSourceParams;
import com.couchbase.client.java.search.SearchQueryHit;
import com.couchbase.client.java.search.SearchQueryResult;
import com.couchbase.client.java.search.alias.AliasIndexParams;
import com.couchbase.client.java.search.alias.AliasIndexSettings;
import com.couchbase.client.java.search.fulltext.DocumentMapping;
import com.couchbase.client.java.search.fulltext.FieldMapping;
import com.couchbase.client.java.search.fulltext.FullTextIndexMapping;
import com.couchbase.client.java.search.fulltext.FullTextIndexParams;
import com.couchbase.client.java.search.fulltext.FullTextIndexSettings;
import com.couchbase.client.java.search.query.BooleanQuery;
import com.couchbase.client.java.search.query.ConjunctionQuery;
import com.couchbase.client.java.search.query.DateRangeQuery;
import com.couchbase.client.java.search.query.DisjunctionQuery;
import com.couchbase.client.java.search.query.FuzzyQuery;
import com.couchbase.client.java.search.query.MatchPhraseQuery;
import com.couchbase.client.java.search.query.MatchQuery;
import com.couchbase.client.java.search.query.NumericRangeQuery;
import com.couchbase.client.java.search.query.PhraseQuery;
import com.couchbase.client.java.search.query.PrefixQuery;
import com.couchbase.client.java.search.query.RegexpQuery;
import com.couchbase.client.java.search.query.SearchQuery;
import com.couchbase.client.java.search.query.StringQuery;
import com.couchbase.client.java.search.query.TermQuery;

import java.util.Calendar;
import java.util.Date;

/**
 * @author Sergey Avseyev
 */

public class DemoCBFT {
    public static void createTravelIndex(BucketManager manager) {
        if (manager.hasSearchIndex("travelIndex")) {
            manager.removeSearchIndex("travelIndex");
        }
        FullTextIndexSettings travelSettings = FullTextIndexSettings.builder()
                .name("travelIndex")
                .params(FullTextIndexParams.builder()
                        .mapping(FullTextIndexMapping.builder()
                                .typeField("type")
                                .type("landmark", DocumentMapping.builder()
                                        .defaultAnalyzer("standard")
                                        .field(FieldMapping.builder().name("name").analyzer("en").build())
                                        .field(FieldMapping.builder().name("title").analyzer("en").build())
                                        .field(FieldMapping.builder().name("geo").index(false).build())
                                        .field(FieldMapping.builder().name("image").index(false).build())
                                        .build())
                                .build())
                        .build())
                .sourceName("travel-sample")
                .sourceType("couchbase")
                .sourceParams(DCPSourceParams.builder()
                        .authUser("travel-sample")
                        .build())
                .build();
        manager.insertSearchIndex(travelSettings);
    }

    public static void createBeerIndex(BucketManager manager) {
        if (manager.hasSearchIndex("beerIndex")) {
            manager.removeSearchIndex("beerIndex");
        }
        FullTextIndexSettings beerSettings = FullTextIndexSettings.builder()
                .name("beerIndex")
                .params(FullTextIndexParams.builder()
                        .mapping(FullTextIndexMapping.builder()
                                .typeField("type")
                                .type("brewery", DocumentMapping.builder()
                                        .defaultAnalyzer("standard")
                                        .field(FieldMapping.builder().name("name").analyzer("en").build())
                                        .field(FieldMapping.builder().name("city").analyzer("en").build())
                                        .field(FieldMapping.builder().name("state").analyzer("en").build())
                                        .field(FieldMapping.builder().name("country").analyzer("en").build())
                                        .field(FieldMapping.builder().name("description").analyzer("en").build())
                                        .field(FieldMapping.builder().name("code").index(false).build())
                                        .field(FieldMapping.builder().name("address").index(false).build())
                                        .field(FieldMapping.builder().name("updated").index(false).build())
                                        .field(FieldMapping.builder().name("phone").index(false).build())
                                        .field(FieldMapping.builder().name("website").index(false).build())
                                        .field(FieldMapping.builder().name("geo").index(false).build())
                                        .build())
                                .type("beer", DocumentMapping.builder()
                                        .defaultAnalyzer("standard")
                                        .field(FieldMapping.builder().name("name").analyzer("en").build())
                                        .field(FieldMapping.builder().name("style").analyzer("en").build())
                                        .field(FieldMapping.builder().name("description").analyzer("en").build())
                                        .field(FieldMapping.builder().name("abv").type("number").build())
                                        .field(FieldMapping.builder().name("ibu").type("number").build())
                                        .field(FieldMapping.builder().name("srm").type("number").build())
                                        .field(FieldMapping.builder().name("upc").type("number").build())
                                        .field(FieldMapping.builder().name("updated").type("datetime").build())
                                        .field(FieldMapping.builder().name("brewery_id").index(false).build())
                                        .build())
                                .build())
                        .build())
                .sourceName("beer-sample")
                .sourceType("couchbase")
                .sourceParams(DCPSourceParams.builder()
                        .authUser("beer-sample")
                        .build())
                .build();
        manager.insertSearchIndex(beerSettings);
    }

    public static void createCommonIndex(BucketManager manager) {
        if (manager.hasSearchIndex("commonIndex")) {
            manager.removeSearchIndex("commonIndex");
        }
        AliasIndexSettings commonSettings = AliasIndexSettings.builder()
                .name("commonIndex")
                .params(AliasIndexParams.builder()
                        .target("travelIndex")
                        .target("beerIndex")
                        .build())
                .build();
        manager.insertSearchIndex(commonSettings);
    }


    public static void main(String[] args) throws InterruptedException {
        DefaultCouchbaseEnvironment.Builder builder = DefaultCouchbaseEnvironment.builder();
        CouchbaseEnvironment environment = builder.build();

        final CouchbaseCluster cluster = CouchbaseCluster.create(environment, "127.0.0.1:8091");
        Bucket bucket = cluster.openBucket();
        BucketManager manager = bucket.bucketManager();

        createBeerIndex(manager);
        createTravelIndex(manager);
        createCommonIndex(manager);

        Thread.sleep(10000);

        SearchQueryResult result;

        result = bucket.search(StringQuery.on("beerIndex").query("national").size(3).fields("*"));
        System.out.println("totalHits: " + result.totalHits());
        for (SearchQueryHit hit : result) {
            System.out.println(hit);
        }

        result = bucket.search(RegexpQuery.on("beerIndex").regexp("[tp]ale").field("name"));
        System.out.println("totalHits: " + result.totalHits());
        for (SearchQueryHit hit : result) {
            System.out.println(hit);
        }


        result = bucket.search(PrefixQuery.on("beerIndex").prefix("weiss").field("name"));
        System.out.println("totalHits: " + result.totalHits());
        for (SearchQueryHit hit : result) {
            System.out.println(hit);
        }

        result = bucket.search(NumericRangeQuery.on("beerIndex").min(3).max(4).field("abv").fields("name", "abv"));
        System.out.println("totalHits: " + result.totalHits());
        for (SearchQueryHit hit : result) {
            System.out.println("\"" + hit.fields().get("name") + "\", abv: " + hit.fields().get("abv"));
        }

        Calendar calendar = Calendar.getInstance();
        calendar.set(2011, Calendar.MARCH, 1);
        Date start = calendar.getTime();
        calendar.set(2011, Calendar.APRIL, 1);
        Date end = calendar.getTime();
        result = bucket.search(DateRangeQuery.on("beerIndex").start(start).end(end).field("updated").fields("name", "updated"));
        System.out.println("totalHits: " + result.totalHits());
        for (SearchQueryHit hit : result) {
            System.out.println("\"" + hit.fields().get("name") + "\", updated: " + hit.fields().get("updated"));
        }

        result = bucket.search(TermQuery.on("beerIndex").term("summer").field("name"));
        System.out.println("totalHits: " + result.totalHits());
        for (SearchQueryHit hit : result) {
            System.out.println(hit);
        }

        result = bucket.search(FuzzyQuery.on("beerIndex").term("sammar").field("name").fuzziness(2));
        System.out.println("totalHits (fuzziness = 2): " + result.totalHits());
        for (SearchQueryHit hit : result) {
            System.out.println(hit);
        }

        result = bucket.search(FuzzyQuery.on("beerIndex").term("sammar").field("name").fuzziness(1));
        System.out.println("totalHits (fuzziness = 1): " + result.totalHits());
        for (SearchQueryHit hit : result) {
            System.out.println(hit);
        }

        SearchQuery bitterQuery;
        SearchQuery maltyQuery;

        // FIXME: Timeout
        bitterQuery = TermQuery.on("beerIndex").term("bitter").field("description").build();
        maltyQuery = TermQuery.on("beerIndex").term("malty").field("description").build();
        result = bucket.search(ConjunctionQuery.on("beerIndex").conjuncts(bitterQuery, maltyQuery));
        System.out.println("totalHits: " + result.totalHits());
        for (SearchQueryHit hit : result) {
            System.out.println(hit);
        }

        bitterQuery = TermQuery.on("beerIndex").term("bitter").field("description").build();
        maltyQuery = TermQuery.on("beerIndex").term("malty").field("description").build();
        result = bucket.search(DisjunctionQuery.on("beerIndex").disjuncts(bitterQuery, maltyQuery));
        System.out.println("totalHits: " + result.totalHits());
        for (SearchQueryHit hit : result) {
            System.out.println(hit);
        }

        result = bucket.search(PhraseQuery.on("beerIndex").terms("bitter", "malty").field("description"));
        System.out.println("totalHits: " + result.totalHits());
        for (SearchQueryHit hit : result) {
            System.out.println(hit);
        }

        bitterQuery = TermQuery.on("beerIndex").term("bitter").field("description").build();
        maltyQuery = TermQuery.on("beerIndex").term("malty").field("description").build();
        result = bucket.search(BooleanQuery.on("beerIndex").must(bitterQuery).mustNot(maltyQuery));
        System.out.println("totalHits: " + result.totalHits());
        for (SearchQueryHit hit : result) {
            System.out.println(hit);
        }

        result = bucket.search(MatchQuery.on("beerIndex").match("sammar sesonal").fuzziness(2).field("description"));
        System.out.println("totalHits: " + result.totalHits());
        for (SearchQueryHit hit : result) {
            System.out.println(hit);
        }

        // FIXME: Timeout
        result = bucket.search(MatchPhraseQuery.on("beerIndex").matchPhrase("summer seasonal").field("description"));
        System.out.println("totalHits: " + result.totalHits());
        for (SearchQueryHit hit : result) {
            System.out.println(hit);
        }

        result = bucket.search(StringQuery.on("commonIndex").query("California"));
        System.out.println("totalHits: " + result.totalHits());
        for (SearchQueryHit hit : result) {
            System.out.println(hit);
        }

    }


}
