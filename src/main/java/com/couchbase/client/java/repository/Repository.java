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

import com.couchbase.client.java.PersistTo;
import com.couchbase.client.java.ReplicaMode;
import com.couchbase.client.java.ReplicateTo;

import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * The repository abstraction for entities on top of a bucket.
 *
 * @author Michael Nitschinger
 * @since 2.2.0
 */
public interface Repository {

    AsyncRepository async();

    <T> T get(String id, Class<T> documentClass);
    <T> T get(String id, Class<T> documentClass, long timeout, TimeUnit timeUnit);

    <T> List<T> getFromReplica(String id, ReplicaMode type, Class<T> documentClass);
    <T> List<T> getFromReplica(String id, ReplicaMode type, Class<T> documentClass, long timeout, TimeUnit timeUnit);

    <T> T getAndLock(String id, int lockTime, Class<T> documentClass);
    <T> T getAndLock(String id, int lockTime, Class<T> documentClass, long timeout, TimeUnit timeUnit);

    <T> T getAndTouch(String id, int expiry, Class<T> documentClass);
    <T> T getAndTouch(String id, int expiry, Class<T> documentClass, long timeout, TimeUnit timeUnit);

    boolean exists(String id);
    boolean exists(String id, long timeout, TimeUnit timeUnit);
    <T> boolean exists(T document);
    <T> boolean exists(T document, long timeout, TimeUnit timeUnit);

    <T> T upsert(T document);
    <T> T upsert(T document, long timeout, TimeUnit timeUnit);
    <T> T upsert(T document, PersistTo persistTo);
    <T> T upsert(T document, PersistTo persistTo, long timeout, TimeUnit timeUnit);
    <T> T upsert(T document, ReplicateTo replicateTo);
    <T> T upsert(T document, ReplicateTo replicateTo, long timeout, TimeUnit timeUnit);
    <T> T upsert(T document, PersistTo persistTo, ReplicateTo replicateTo);
    <T> T upsert(T document, PersistTo persistTo, ReplicateTo replicateTo, long timeout, TimeUnit timeUnit);

    <T> T insert(T document);
    <T> T insert(T document, long timeout, TimeUnit timeUnit);
    <T> T insert(T document, PersistTo persistTo);
    <T> T insert(T document, PersistTo persistTo, long timeout, TimeUnit timeUnit);
    <T> T insert(T document, ReplicateTo replicateTo);
    <T> T insert(T document, ReplicateTo replicateTo, long timeout, TimeUnit timeUnit);
    <T> T insert(T document, PersistTo persistTo, ReplicateTo replicateTo);
    <T> T insert(T document, PersistTo persistTo, ReplicateTo replicateTo, long timeout, TimeUnit timeUnit);

    <T> T replace(T document);
    <T> T replace(T document, long timeout, TimeUnit timeUnit);
    <T> T replace(T document, PersistTo persistTo);
    <T> T replace(T document, PersistTo persistTo, long timeout, TimeUnit timeUnit);
    <T> T replace(T document, ReplicateTo replicateTo);
    <T> T replace(T document, ReplicateTo replicateTo, long timeout, TimeUnit timeUnit);
    <T> T replace(T document, PersistTo persistTo, ReplicateTo replicateTo);
    <T> T replace(T document, PersistTo persistTo, ReplicateTo replicateTo, long timeout, TimeUnit timeUnit);

    <T> T remove(T document);
    <T> T remove(T document, long timeout, TimeUnit timeUnit);
    <T> T remove(T document, PersistTo persistTo);
    <T> T remove(T document, PersistTo persistTo, long timeout, TimeUnit timeUnit);
    <T> T remove(T document, ReplicateTo replicateTo);
    <T> T remove(T document, ReplicateTo replicateTo, long timeout, TimeUnit timeUnit);
    <T> T remove(T document, PersistTo persistTo, ReplicateTo replicateTo);
    <T> T remove(T document, PersistTo persistTo, ReplicateTo replicateTo, long timeout, TimeUnit timeUnit);
    <T> T remove(String id, Class<T> documentClass);
    <T> T remove(String id, Class<T> documentClass, long timeout, TimeUnit timeUnit);
    <T> T remove(String id, PersistTo persistTo, Class<T> documentClass);
    <T> T remove(String id, PersistTo persistTo, Class<T> documentClass, long timeout, TimeUnit timeUnit);
    <T> T remove(String id, ReplicateTo replicateTo, Class<T> documentClass);
    <T> T remove(String id, ReplicateTo replicateTo, Class<T> documentClass, long timeout, TimeUnit timeUnit);
    <T> T remove(String id, PersistTo persistTo, ReplicateTo replicateTo, Class<T> documentClass);
    <T> T remove(String id, PersistTo persistTo, ReplicateTo replicateTo, Class<T> documentClass, long timeout, TimeUnit timeUnit);

}
