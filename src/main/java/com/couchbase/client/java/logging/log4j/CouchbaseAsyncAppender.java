package com.couchbase.client.java.logging.log4j;

import com.couchbase.client.core.annotations.InterfaceAudience;
import com.couchbase.client.core.annotations.InterfaceStability;
import com.couchbase.client.java.document.JsonDocument;
import org.apache.log4j.helpers.LogLog;
import rx.functions.Action1;

/**
 * A Couchbase-backed synchronous Log4j Appender.
 *
 * @author Michael Nitschinger
 * @since 2.0.2
 */
@InterfaceStability.Experimental
@InterfaceAudience.Public
public class CouchbaseAsyncAppender extends AbstractAppender {

    public CouchbaseAsyncAppender() {
    }

    public CouchbaseAsyncAppender(boolean isActive) {
        super(isActive);
    }

    @Override
    protected void doAppend(final JsonDocument document) {
        getBucketRef()
            .async()
            .insert(document)
            .doOnError(new Action1<Throwable>() {
                @Override
                public void call(Throwable ex) {
                    LogLog.error("Could not Log message", ex);
                    errorHandler.error("Could not Log Message" + ex);
                }
            })
            .subscribe();
    }
}
