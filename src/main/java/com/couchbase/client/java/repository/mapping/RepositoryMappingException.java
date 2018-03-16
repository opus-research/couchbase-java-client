package com.couchbase.client.java.repository.mapping;

import com.couchbase.client.core.CouchbaseException;

public class RepositoryMappingException extends CouchbaseException {

    public RepositoryMappingException() {
    }

    public RepositoryMappingException(String message) {
        super(message);
    }

    public RepositoryMappingException(String message, Throwable cause) {
        super(message, cause);
    }

    public RepositoryMappingException(Throwable cause) {
        super(cause);
    }
}
