/*
 * Copyright (c) 2016 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.couchbase.client.java.datastructures;

import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;

import com.couchbase.client.core.BackpressureException;
import com.couchbase.client.core.CouchbaseException;
import com.couchbase.client.core.RequestCancelledException;
import com.couchbase.client.core.annotations.InterfaceAudience;
import com.couchbase.client.core.annotations.InterfaceStability;
import com.couchbase.client.core.message.ResponseStatus;
import com.couchbase.client.core.message.kv.subdoc.multi.Lookup;
import com.couchbase.client.core.message.kv.subdoc.multi.Mutation;
import com.couchbase.client.java.CouchbaseAsyncBucket;
import com.couchbase.client.java.PersistTo;
import com.couchbase.client.java.ReplicateTo;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.client.java.error.*;
import com.couchbase.client.java.error.subdoc.MultiMutationException;
import com.couchbase.client.java.subdoc.DocumentFragment;
import com.couchbase.client.java.subdoc.SubdocOperationResult;
import com.couchbase.client.java.Bucket;
import rx.Observable;
import rx.functions.Func0;
import rx.functions.Func1;

/**
 * CouchbaseMap is a data structure backed by a JsonDocument.
 * It has a map collection like interface supporting operations that can be executed asynchronously
 * against a Couchbase Server bucket. Key is a string.
 *
 * @param <V> Type of the value
 * @author subhashni
 */
@InterfaceStability.Experimental
@InterfaceAudience.Public
public class CouchbaseMap<V> {

    private final String docId;
    private final CouchbaseAsyncBucket bucket;

    /**
     * Create {@link Bucket Couchbase-backed} CouchbaseMap, backed by the document identified by <code>id</code>
     * in <code>bucket</code>. Note that if the document with the data structure already exists,
     * its content will be used as initial content.
     *
     * @param bucket the {@link Bucket} through which to interact with the document.
     * @param docId the id of the Couchbase document to back the list.
     */
    public CouchbaseMap(CouchbaseAsyncBucket bucket, String docId) {
        this.bucket = bucket;
        this.docId = docId;
    }

    private Observable<JsonDocument> createDocument(String key, V value) {
        return bucket.upsert(JsonDocument.create(docId, JsonObject.create().put(key, value)));
    }

    /**
     * Add a key value pair into CouchbaseMap. If the underlying document for the map does not exist,
     * this operation will create a new document to back the data structure.
     * This method throws under the following conditions:
     * - {@link IllegalStateException} if the map is full (limited by couchbase document size)
     * - The producer outpaces the SDK: {@link BackpressureException}
     * - The operation had to be cancelled while on the wire or the retry strategy cancelled it instead of
     * retrying: {@link RequestCancelledException}
     * - The server is currently not able to process the request, retrying may help: {@link TemporaryFailureException}
     * - The server is out of memory: {@link CouchbaseOutOfMemoryException}
     * - Unexpected errors are caught and contained in a generic {@link CouchbaseException}.
     *
     * @param key key to be stored
     * @param value value to be stored
     * @return true if successful
     */
    public Observable<Boolean> add(final String key,
                                   final V value) {
        return add(key, value, MutationOptionBuilder.build());
    }

    /**
     * Add a key value pair into CouchbaseMap with additional mutation options provided by {@link MutationOptionBuilder}.
     * If the underlying document for the map does not exist, this operation will create a new document to back
     * the data structure.
     * This method throws under the following conditions:
     * - {@link IllegalStateException} if the map is full (limited by couchbase document size)
     * - The producer outpaces the SDK: {@link BackpressureException}
     * - The operation had to be cancelled while on the wire or the retry strategy cancelled it instead of
     * retrying: {@link RequestCancelledException}
     * - The durability constraint could not be fulfilled because of a temporary or persistent problem:
     * {@link DurabilityException}.
     * - A CAS value was set and it did not match with the server: {@link CASMismatchException}
     * - The server is currently not able to process the request, retrying may help: {@link TemporaryFailureException}
     * - The server is out of memory: {@link CouchbaseOutOfMemoryException}
     * - Unexpected errors are caught and contained in a generic {@link CouchbaseException}.
     *
     * @param key key to be stored
     * @param value value to be stored
     * @param mutationOptionBuilder mutation options {@link MutationOptionBuilder}
     * @return true if successful
     */
    public Observable<Boolean> add(final String key,
                                   final V value,
                                   final MutationOptionBuilder mutationOptionBuilder) {
        final Func1<DocumentFragment<Mutation>, Boolean> mapResult = new Func1<DocumentFragment<Mutation>, Boolean>() {
            @Override
            public Boolean call(DocumentFragment<Mutation> documentFragment) {
                ResponseStatus status = documentFragment.status(0);
                if (status == ResponseStatus.SUCCESS) {
                    return true;
                } else {
                    if (status == ResponseStatus.TOO_BIG) {
                        throw new IllegalStateException("Map full");
                    } else {
                        throw new CouchbaseException(status.toString());
                    }
                }
            }
        };

        return Observable.defer(new Func0<Observable<Boolean>>() {
            @Override
            public Observable<Boolean> call() {
                return subdocAdd(key, value, mutationOptionBuilder.cas, mutationOptionBuilder.persistTo,
                        mutationOptionBuilder.replicateTo, mutationOptionBuilder.expiry)
                        .map(mapResult);
            }
        });
    }

    private Observable<DocumentFragment<Mutation>> subdocAdd(final String key,
                                                             final V value,
                                                             final long cas,
                                                             final PersistTo persistTo,
                                                             final ReplicateTo replicateTo,
                                                             final int expiry) {

        final Func1<JsonDocument, DocumentFragment<Mutation>> mapFullDocResultToSubdoc = new
                Func1<JsonDocument, DocumentFragment<Mutation>>() {
                    @Override
                    public DocumentFragment<Mutation> call(JsonDocument document) {
                        List<SubdocOperationResult<Mutation>> list;
                        list = new ArrayList<SubdocOperationResult<Mutation>>();
                        list.add(SubdocOperationResult.createResult(key, Mutation.DICT_UPSERT,
                                ResponseStatus.SUCCESS, value));
                        return new DocumentFragment<Mutation>(document.id(), document.cas(),
                                document.mutationToken(),
                                list);
                    }
                };

        final Func1<Throwable, Observable<? extends DocumentFragment<Mutation>>> retryAddIfDocExists = new
                Func1<Throwable, Observable<? extends DocumentFragment<Mutation>>>() {
                    @Override
                    public Observable<? extends DocumentFragment<Mutation>> call(Throwable throwable) {
                        if (throwable instanceof DocumentAlreadyExistsException) {
                            return subdocAdd(key, value, cas, persistTo, replicateTo,
                                    expiry);
                        } else {
                            return Observable.error(throwable);
                        }
                    }
                };

        return bucket.mutateIn(docId).insert(key, value, false)
                .withCas(cas)
                .withDurability(persistTo, replicateTo)
                .withExpiry(expiry)
                .execute()
                .onErrorResumeNext(new Func1<Throwable, Observable<? extends DocumentFragment<Mutation>>>() {
                    @Override
                    public Observable<? extends DocumentFragment<Mutation>> call(Throwable throwable) {
                        if (throwable instanceof DocumentDoesNotExistException) {
                            return createDocument(key, value)
                                    .map(mapFullDocResultToSubdoc)
                                    .onErrorResumeNext(retryAddIfDocExists);
                        } else {
                            if (throwable instanceof MultiMutationException) {
                                //Wrap it up a subdoc result, since we dont want to throw it back as subdoc exception
                                ResponseStatus status = ((MultiMutationException) throwable).firstFailureStatus();
                                List<SubdocOperationResult<Mutation>> list;
                                list = new ArrayList<SubdocOperationResult<Mutation>>();
                                list.add(SubdocOperationResult.createResult(null, Mutation.DICT_UPSERT, status, null));
                                return Observable.just(new DocumentFragment<Mutation>(null, 0, null, list));
                            } else {
                                return Observable.error(throwable);
                            }
                        }
                    }
                });
    }

    /**
     * Get value of a key in the CouchbaseMap.
     * This method throws under the following conditions:
     * - {@link NoSuchElementException} if key is not found in map
     * - The producer outpaces the SDK: {@link BackpressureException}
     * - The operation had to be cancelled while on the wire or the retry strategy cancelled it instead of
     * retrying: {@link RequestCancelledException}
     * - If the underlying couchbase document does not exist: {@link DocumentDoesNotExistException}
     * - The server is currently not able to process the request, retrying may help: {@link TemporaryFailureException}
     * - The server is out of memory: {@link CouchbaseOutOfMemoryException}
     * - Unexpected errors are caught and contained in a generic {@link CouchbaseException}.
     *
     * @param key key in the map
     * @return value if found
     */
    public Observable<V> get(final String key) {
        final Func1<DocumentFragment<Lookup>, V> mapResult = new Func1<DocumentFragment<Lookup>, V>() {
            @Override
            public V call(DocumentFragment<Lookup> documentFragment) {
                ResponseStatus status = documentFragment.status(0);
                if (status == ResponseStatus.SUCCESS) {
                    return (V) documentFragment.content(0);
                } else {
                    if (status == ResponseStatus.SUBDOC_PATH_NOT_FOUND) {
                        throw new NoSuchElementException();
                    } else {
                        throw new CouchbaseException(status.toString());
                    }
                }
            }
        };
        return Observable.defer(new Func0<Observable<V>>() {
            @Override
            public Observable<V> call() {
                return bucket.lookupIn(docId).get(key)
                        .execute()
                        .map(mapResult);
            }
        });
    }

    /**
     * Remove a key value pair from CouchbaseMap.
     * This method throws under the following conditions:
     * - The producer outpaces the SDK: {@link BackpressureException}
     * - The operation had to be cancelled while on the wire or the retry strategy cancelled it instead of
     * retrying: {@link RequestCancelledException}
     * - If the underlying couchbase document does not exist: {@link DocumentDoesNotExistException}
     * - The server is currently not able to process the request, retrying may help: {@link TemporaryFailureException}
     * - The server is out of memory: {@link CouchbaseOutOfMemoryException}
     * - Unexpected errors are caught and contained in a generic {@link CouchbaseException}.
     *
     * @param key key to be removed
     * @return true if successful, even if the key doesn't exist
     */
    public Observable<Boolean> remove(final String key) {
        return remove(key, MutationOptionBuilder.build());
    }

    /**
     * Remove a key value pair from CouchbaseMap with additional mutation options provided by {@link MutationOptionBuilder}.
     * This method throws under the following conditions:
     * - The producer outpaces the SDK: {@link BackpressureException}
     * - The operation had to be cancelled while on the wire or the retry strategy cancelled it instead of
     * retrying: {@link RequestCancelledException}
     * - If the underlying couchbase document does not exist: {@link DocumentDoesNotExistException}
     * - The durability constraint could not be fulfilled because of a temporary or persistent problem:
     * {@link DurabilityException}.
     * - A CAS value was set and it did not match with the server: {@link CASMismatchException}
     * - The server is currently not able to process the request, retrying may help: {@link TemporaryFailureException}
     * - The server is out of memory: {@link CouchbaseOutOfMemoryException}
     * - Unexpected errors are caught and contained in a generic {@link CouchbaseException}.
     *
     * @param key key to be removed
     * @return true if successful, even if the key doesn't exist
     */
    public Observable<Boolean> remove(final String key, final MutationOptionBuilder mutationOptionBuilder) {
        final Func1<DocumentFragment<Mutation>, Boolean> mapResult = new Func1<DocumentFragment<Mutation>, Boolean>() {
            @Override
            public Boolean call(DocumentFragment<Mutation> documentFragment) {
                ResponseStatus status = documentFragment.status(0);
                if (status == ResponseStatus.SUCCESS) {
                    return true;
                } else {
                    return false;
                }
            }
        };

        final Func1<Throwable, Observable<? extends Boolean>> handleSubdocException = new
                Func1<Throwable, Observable<? extends Boolean>>() {
                    @Override
                    public Observable<? extends Boolean> call(Throwable throwable) {
                        ResponseStatus status = ((MultiMutationException) throwable).firstFailureStatus();
                        if (status == ResponseStatus.SUBDOC_PATH_NOT_FOUND) {
                            return Observable.just(true);
                        } else {
                            throw new CouchbaseException(status.toString());
                        }
                    }
                };

        return Observable.defer(new Func0<Observable<Boolean>>() {
            @Override
            public Observable<Boolean> call() {
                return bucket.mutateIn(docId).remove(key)
                        .withCas(mutationOptionBuilder.cas)
                        .withDurability(mutationOptionBuilder.persistTo, mutationOptionBuilder.replicateTo)
                        .withExpiry(mutationOptionBuilder.expiry)
                        .execute()
                        .map(mapResult)
                        .onErrorResumeNext(handleSubdocException);
            }
        });
    }

    /**
     * Returns the number key value pairs in CouchbaseMap
     * This method throws under the following conditions:
     * - The producer outpaces the SDK: {@link BackpressureException}
     * - The operation had to be cancelled while on the wire or the retry strategy cancelled it instead of
     * retrying: {@link RequestCancelledException}
     * - If the underlying couchbase document does not exist: {@link DocumentDoesNotExistException}
     * - The server is currently not able to process the request, retrying may help: {@link TemporaryFailureException}
     * - The server is out of memory: {@link CouchbaseOutOfMemoryException}
     * - Unexpected errors are caught and contained in a generic {@link CouchbaseException}.
     *
     * @return number of key value pairs
     */
    public Observable<Integer> size() {
        return Observable.defer(new Func0<Observable<Integer>>() {
            @Override
            public Observable<Integer> call() {
                return bucket.get(docId, JsonDocument.class)
                        .map(new Func1<JsonDocument, Integer>() {
                            @Override
                            public Integer call(JsonDocument document) {
                                return document.content().size();
                            }
                        });
            }
        });
    }
}