package com.couchbase.client;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public interface CouchbaseN1qlClientIF {

    Future<N1qlResponseOld> asyncQuery(String query) throws IOException;

    N1qlResponseOld query(String query) throws IOException, ExecutionException, InterruptedException;

    boolean close();
}
