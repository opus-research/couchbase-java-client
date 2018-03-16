package com.couchbase.client.java.logging.log4j;

import com.couchbase.client.core.annotations.InterfaceAudience;
import com.couchbase.client.core.annotations.InterfaceStability;
import com.couchbase.client.java.document.JsonDocument;
import org.apache.log4j.helpers.LogLog;

/**
 * A Couchbase-backed synchronous Log4j Appender.
 *
 * @author Michael Nitschinger
 * @since 2.0.2
 */
@InterfaceStability.Experimental
@InterfaceAudience.Public
public class CouchbaseAppender extends AbstractAppender {

    public CouchbaseAppender() {
    }

    public CouchbaseAppender(boolean isActive) {
        super(isActive);
    }

    @Override
    protected void doAppend(final JsonDocument document) {
        try {
            getBucketRef().insert(document);
        } catch(Exception ex) {
            LogLog.error("Could not Log message", ex);
            errorHandler.error("Could not Log Message" + ex);
        }
    }
}
