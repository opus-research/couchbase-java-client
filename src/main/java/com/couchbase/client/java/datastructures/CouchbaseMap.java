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

import com.couchbase.client.core.CouchbaseException;
import com.couchbase.client.core.message.ResponseStatus;
import com.couchbase.client.core.message.kv.subdoc.multi.Lookup;
import com.couchbase.client.core.message.kv.subdoc.multi.Mutation;
import com.couchbase.client.java.CouchbaseAsyncBucket;
import com.couchbase.client.java.PersistTo;
import com.couchbase.client.java.ReplicateTo;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.client.java.error.DocumentAlreadyExistsException;
import com.couchbase.client.java.error.DocumentDoesNotExistException;
import com.couchbase.client.java.error.subdoc.MultiMutationException;
import com.couchbase.client.java.subdoc.DocumentFragment;
import com.couchbase.client.java.subdoc.SubdocOperationResult;
import rx.Observable;
import rx.functions.Func0;
import rx.functions.Func1;

/**
 * Couchbase map is a data structure backed by a JsonDocument
 * It has a collection like interface where key is a string
 *
 * @param <V> Type of the value
 * @author subhashni
 */
public class CouchbaseMap<V> {

    private final String docId;
    private final CouchbaseAsyncBucket bucket;

    public CouchbaseMap(CouchbaseAsyncBucket bucket, String docId) {
        this.bucket = bucket;
        this.docId = docId;
    }

    private Observable<JsonDocument> createDocument(String key, V value) {
        return bucket.upsert(JsonDocument.create(docId, JsonObject.create().put(key, value)));
    }

    public Observable<Boolean> add(final String key,
                                   final V value) {
        return add(key, value, MutationOptionBuilder.build());
    }

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

    public Observable<Boolean> remove(final String key) {
        return remove(key, MutationOptionBuilder.build());
    }

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
                            return Observable.just(false);
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