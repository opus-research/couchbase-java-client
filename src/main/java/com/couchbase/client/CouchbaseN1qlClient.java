package com.couchbase.client;

import com.couchbase.client.protocol.n1ql.N1qlResponseHandler;
import com.ning.http.client.*;
import org.apache.http.*;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.logging.Level;
import java.util.logging.Logger;

public class CouchbaseN1qlClient implements CouchbaseN1qlClientIF {
    private static final Logger LOGGER = Logger.getLogger(CouchbaseN1qlClient.class.getName());
    private static final String QUERY_URI = "/query";
    private HttpHost host;
    private AsyncHttpClient client = new AsyncHttpClient();

    public CouchbaseN1qlClient(String hostName, int port) {
        host = new HttpHost(hostName, port);
    }


    @Override
    public ListenableFuture<N1qlResponseOld> asyncQuery(String query) throws IOException {
        LOGGER.log(Level.INFO, "Connecting to: " + host.toURI());
        FluentStringsMap params = new FluentStringsMap().add("q", query);
        LOGGER.log(Level.INFO, "Executing Query: " + query);
        return client.preparePost(host.toURI() + QUERY_URI)
                .setParameters(params)
                .execute(new N1qlResponseHandler());
    }

    @Override
    public N1qlResponseOld query(String query) throws IOException, ExecutionException, InterruptedException {
        return asyncQuery(query).get();
    }

    @Override
    public boolean close() {
        if (client != null && !client.isClosed())
            client.close();
        return true;
    }
}
