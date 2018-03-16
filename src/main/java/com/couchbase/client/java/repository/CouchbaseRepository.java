/**
 * Copyright (C) 2015 Couchbase, Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALING
 * IN THE SOFTWARE.
 */
package com.couchbase.client.java.repository;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.PersistTo;
import com.couchbase.client.java.ReplicaMode;
import com.couchbase.client.java.ReplicateTo;
import com.couchbase.client.java.env.CouchbaseEnvironment;
import com.couchbase.client.java.util.Blocking;

import java.util.List;
import java.util.concurrent.TimeUnit;

public class CouchbaseRepository implements Repository {

    private static final TimeUnit TIMEOUT_UNIT = TimeUnit.MILLISECONDS;

    private final AsyncRepository asyncRepository;
    private final long timeout;

    public CouchbaseRepository(Bucket bucket, CouchbaseEnvironment environment) {
        this.timeout = environment.kvTimeout();
        this.asyncRepository = bucket.async().repository().toBlocking().single();
    }

    @Override
    public AsyncRepository async() {
        return asyncRepository;
    }

    @Override
    public <T> T get(String id, Class<T> entityClass) {
        return get(id, entityClass, timeout, TIMEOUT_UNIT);
    }

    @Override
    public <T> T get(String id, Class<T> entityClass, long timeout, TimeUnit timeUnit) {
        return Blocking.blockForSingle(asyncRepository.get(id, entityClass), timeout, timeUnit);
    }

    @Override
    public <T> T upsert(T document) {
        return upsert(document, timeout, TIMEOUT_UNIT);
    }

    @Override
    public <T> T upsert(T document, long timeout, TimeUnit timeUnit) {
        return Blocking.blockForSingle(asyncRepository.upsert(document), timeout, timeUnit);
    }

    @Override
    public <T> List<T> getFromReplica(String id, ReplicaMode type, Class<T> documentClass) {
        return getFromReplica(id, type, documentClass, timeout, TIMEOUT_UNIT);
    }

    @Override
    public <T> List<T> getFromReplica(String id, ReplicaMode type, Class<T> documentClass, long timeout, TimeUnit timeUnit) {
        return Blocking.blockForSingle(asyncRepository.getFromReplica(id, type, documentClass).toList(), timeout, timeUnit);
    }

    @Override
    public <T> T getAndLock(String id, int lockTime, Class<T> documentClass) {
        return getAndLock(id, lockTime, documentClass, timeout, TIMEOUT_UNIT);
    }

    @Override
    public <T> T getAndLock(String id, int lockTime, Class<T> documentClass, long timeout, TimeUnit timeUnit) {
        return Blocking.blockForSingle(asyncRepository.getAndLock(id, lockTime, documentClass), timeout, timeUnit);
    }

    @Override
    public <T> T getAndTouch(String id, int expiry, Class<T> documentClass) {
        return getAndTouch(id, expiry, documentClass, timeout, TIMEOUT_UNIT);
    }

    @Override
    public <T> T getAndTouch(String id, int expiry, Class<T> documentClass, long timeout, TimeUnit timeUnit) {
        return Blocking.blockForSingle(asyncRepository.getAndTouch(id, expiry, documentClass), timeout, timeUnit);
    }

    @Override
    public boolean exists(String id) {
        return exists(id, timeout, TIMEOUT_UNIT);
    }

    @Override
    public boolean exists(String id, long timeout, TimeUnit timeUnit) {
        return Blocking.blockForSingle(asyncRepository.exists(id), timeout, timeUnit);
    }

    @Override
    public <T> boolean exists(T document) {
        return exists(document, timeout, TIMEOUT_UNIT);
    }

    @Override
    public <T> boolean exists(T document, long timeout, TimeUnit timeUnit) {
        return Blocking.blockForSingle(asyncRepository.exists(document), timeout, timeUnit);
    }

    @Override
    public <T> T upsert(T document, PersistTo persistTo) {
        return upsert(document, persistTo, timeout, TIMEOUT_UNIT);
    }

    @Override
    public <T> T upsert(T document, PersistTo persistTo, long timeout, TimeUnit timeUnit) {
        return upsert(document, persistTo, ReplicateTo.NONE, timeout, timeUnit);
    }

    @Override
    public <T> T upsert(T document, ReplicateTo replicateTo) {
        return upsert(document, replicateTo, timeout, TIMEOUT_UNIT);
    }

    @Override
    public <T> T upsert(T document, ReplicateTo replicateTo, long timeout, TimeUnit timeUnit) {
        return upsert(document, PersistTo.NONE, replicateTo, timeout, timeUnit);
    }

    @Override
    public <T> T upsert(T document, PersistTo persistTo, ReplicateTo replicateTo) {
        return upsert(document, persistTo, replicateTo, timeout, TIMEOUT_UNIT);
    }

    @Override
    public <T> T upsert(T document, PersistTo persistTo, ReplicateTo replicateTo, long timeout, TimeUnit timeUnit) {
        return Blocking.blockForSingle(asyncRepository.upsert(document, persistTo, replicateTo), timeout, timeUnit);
    }

    @Override
    public <T> T insert(T document, PersistTo persistTo) {
        return upsert(document, persistTo, timeout, TIMEOUT_UNIT);
    }

    @Override
    public <T> T insert(T document, PersistTo persistTo, long timeout, TimeUnit timeUnit) {
        return upsert(document, persistTo, ReplicateTo.NONE, timeout, timeUnit);
    }

    @Override
    public <T> T insert(T document, ReplicateTo replicateTo) {
        return upsert(document, replicateTo, timeout, TIMEOUT_UNIT);
    }

    @Override
    public <T> T insert(T document, ReplicateTo replicateTo, long timeout, TimeUnit timeUnit) {
        return upsert(document, PersistTo.NONE, replicateTo, timeout, timeUnit);
    }

    @Override
    public <T> T insert(T document, PersistTo persistTo, ReplicateTo replicateTo) {
        return upsert(document, persistTo, replicateTo, timeout, TIMEOUT_UNIT);
    }

    @Override
    public <T> T insert(T document, PersistTo persistTo, ReplicateTo replicateTo, long timeout, TimeUnit timeUnit) {
        return Blocking.blockForSingle(asyncRepository.upsert(document, persistTo, replicateTo), timeout, timeUnit);
    }

    @Override
    public <T> T insert(T document) {
        return insert(document, timeout, TIMEOUT_UNIT);
    }

    @Override
    public <T> T insert(T document, long timeout, TimeUnit timeUnit) {
        return insert(document, PersistTo.NONE, ReplicateTo.NONE, timeout, timeUnit);
    }

    @Override
    public <T> T replace(T document) {
        return replace(document, timeout, TIMEOUT_UNIT);
    }

    @Override
    public <T> T replace(T document, long timeout, TimeUnit timeUnit) {
        return replace(document, PersistTo.NONE, ReplicateTo.NONE, timeout, timeUnit);
    }

    @Override
    public <T> T replace(T document, PersistTo persistTo) {
        return upsert(document, persistTo, timeout, TIMEOUT_UNIT);
    }

    @Override
    public <T> T replace(T document, PersistTo persistTo, long timeout, TimeUnit timeUnit) {
        return upsert(document, persistTo, ReplicateTo.NONE, timeout, timeUnit);
    }

    @Override
    public <T> T replace(T document, ReplicateTo replicateTo) {
        return upsert(document, replicateTo, timeout, TIMEOUT_UNIT);
    }

    @Override
    public <T> T replace(T document, ReplicateTo replicateTo, long timeout, TimeUnit timeUnit) {
        return upsert(document, PersistTo.NONE, replicateTo, timeout, timeUnit);
    }

    @Override
    public <T> T replace(T document, PersistTo persistTo, ReplicateTo replicateTo) {
        return upsert(document, persistTo, replicateTo, timeout, TIMEOUT_UNIT);
    }

    @Override
    public <T> T replace(T document, PersistTo persistTo, ReplicateTo replicateTo, long timeout, TimeUnit timeUnit) {
        return Blocking.blockForSingle(asyncRepository.upsert(document, persistTo, replicateTo), timeout, timeUnit);
    }

    @Override
    public <T> T remove(T document) {
        return remove(document, timeout, TIMEOUT_UNIT);
    }

    @Override
    public <T> T remove(T document, long timeout, TimeUnit timeUnit) {
        return remove(document, PersistTo.NONE, ReplicateTo.NONE, timeout, timeUnit);
    }

    @Override
    public <T> T remove(T document, PersistTo persistTo) {
        return remove(document, persistTo, ReplicateTo.NONE, timeout, TIMEOUT_UNIT);
    }

    @Override
    public <T> T remove(T document, PersistTo persistTo, long timeout, TimeUnit timeUnit) {
        return remove(document, persistTo, ReplicateTo.NONE, timeout, timeUnit);
    }

    @Override
    public <T> T remove(T document, ReplicateTo replicateTo) {
        return remove(document, PersistTo.NONE, replicateTo, timeout, TIMEOUT_UNIT);
    }

    @Override
    public <T> T remove(T document, ReplicateTo replicateTo, long timeout, TimeUnit timeUnit) {
        return remove(document, PersistTo.NONE, replicateTo, timeout, timeUnit);
    }

    @Override
    public <T> T remove(T document, PersistTo persistTo, ReplicateTo replicateTo) {
        return remove(document, persistTo, replicateTo, timeout, TIMEOUT_UNIT);
    }

    @Override
    public <T> T remove(T document, PersistTo persistTo, ReplicateTo replicateTo, long timeout, TimeUnit timeUnit) {
        return Blocking.blockForSingle(asyncRepository.remove(document, persistTo, replicateTo), timeout, timeUnit);
    }

    @Override
    public <T> T remove(String id, Class<T> documentClass) {
        return remove(id, documentClass, timeout, TIMEOUT_UNIT);
    }

    @Override
    public <T> T remove(String id, Class<T> documentClass, long timeout, TimeUnit timeUnit) {
        return remove(id, PersistTo.NONE, ReplicateTo.NONE, documentClass, timeout, timeUnit);
    }

    @Override
    public <T> T remove(String id, PersistTo persistTo, Class<T> documentClass) {
        return remove(id, persistTo, ReplicateTo.NONE, documentClass, timeout, TIMEOUT_UNIT);
    }

    @Override
    public <T> T remove(String id, PersistTo persistTo, Class<T> documentClass, long timeout, TimeUnit timeUnit) {
        return remove(id, persistTo, ReplicateTo.NONE, documentClass, timeout, timeUnit);
    }

    @Override
    public <T> T remove(String id, ReplicateTo replicateTo, Class<T> documentClass) {
        return remove(id, PersistTo.NONE, replicateTo, documentClass, timeout, TIMEOUT_UNIT);
    }

    @Override
    public <T> T remove(String id, ReplicateTo replicateTo, Class<T> documentClass, long timeout, TimeUnit timeUnit) {
        return remove(id, PersistTo.NONE, replicateTo, documentClass, timeout, timeUnit);
    }

    @Override
    public <T> T remove(String id, PersistTo persistTo, ReplicateTo replicateTo, Class<T> documentClass) {
        return remove(id, persistTo, replicateTo, documentClass, timeout, TIMEOUT_UNIT);
    }

    @Override
    public <T> T remove(String id, PersistTo persistTo, ReplicateTo replicateTo, Class<T> documentClass, long timeout, TimeUnit timeUnit) {
        return Blocking.blockForSingle(asyncRepository.remove(id, persistTo, replicateTo, documentClass), timeout, timeUnit);
    }
}
