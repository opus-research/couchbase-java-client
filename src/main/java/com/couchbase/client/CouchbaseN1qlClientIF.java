package com.couchbase.client;

import com.couchbase.client.protocol.n1ql.N1qlResponse;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public interface CouchbaseN1qlClientIF {

    Future<N1qlResponse> asyncQuery(String query) throws IOException;

    N1qlResponse query(String query) throws IOException, ExecutionException, InterruptedException;

    boolean close();
}
