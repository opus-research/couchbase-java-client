package com.couchbase.client.protocol.n1ql;

import com.couchbase.client.internal.HttpFuture;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;

public class N1qlFuture extends HttpFuture<N1qlResponse> {

    public N1qlFuture(CountDownLatch latch, long timeout, ExecutorService service) {
        super(latch, timeout, service);
    }

}
