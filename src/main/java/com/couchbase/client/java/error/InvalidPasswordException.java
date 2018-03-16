package com.couchbase.client.java.error;

import com.couchbase.client.core.CouchbaseException;

/**
 * This exception is commonly raised when the password for a resource does not match.
 *
 * @author Michael Nitschinger
 * @since 2.0
 */
public class InvalidPasswordException extends CouchbaseException {

    public InvalidPasswordException() {
    }

    public InvalidPasswordException(String message) {
        super(message);
    }

    public InvalidPasswordException(String message, Throwable cause) {
        super(message, cause);
    }

    public InvalidPasswordException(Throwable cause) {
        super(cause);
    }
}
