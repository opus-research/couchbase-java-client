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
import java.util.Iterator;
import java.util.List;

import com.couchbase.client.core.BackpressureException;
import com.couchbase.client.core.CouchbaseException;
import com.couchbase.client.core.RequestCancelledException;
import com.couchbase.client.core.message.ResponseStatus;
import com.couchbase.client.core.message.kv.subdoc.multi.Mutation;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.CouchbaseAsyncBucket;
import com.couchbase.client.java.PersistTo;
import com.couchbase.client.java.ReplicateTo;
import com.couchbase.client.java.document.JsonArrayDocument;
import com.couchbase.client.java.document.json.JsonArray;
import com.couchbase.client.java.error.*;
import com.couchbase.client.java.error.subdoc.MultiMutationException;
import com.couchbase.client.java.subdoc.DocumentFragment;
import com.couchbase.client.java.subdoc.SubdocOperationResult;
import rx.Observable;
import rx.functions.Func0;
import rx.functions.Func1;

/**
 * Couchbase set data structure backed by JsonArrayDocument.
 * It has set collection like interface supporting operations that can be executed asynchronously
 * against a Couchbase Server bucket.
 *
 * @param <E> Type of element
 * @author subhashni
 */
public class CouchbaseSet<E> {

    private final String docId;
    private final CouchbaseAsyncBucket bucket;

    /**
     * Create {@link Bucket Couchbase-backed} CouchbaseSet, backed by the document identified by <code>id</code>
     * in <code>bucket</code>. Note that if the document with the data structure already exists,
     * its content will be used as initial content.
     *
     * @param bucket the {@link Bucket} through which to interact with the document.
     * @param docId the id of the Couchbase document to back the list.
     */
    public CouchbaseSet(CouchbaseAsyncBucket bucket, String docId) {
        this.bucket = bucket;
        this.docId = docId;
    }

    private Observable<JsonArrayDocument> createDocument(E element) {
        return bucket.upsert(JsonArrayDocument.create(docId, JsonArray.create().add(element)));
    }

    /**
     * Add an element into CouchbaseSet. If the underlying document for the set does not exist,
     * this operation will create a new document to back the data structure.
     * This method throws under the following conditions:
     * - {@link IllegalStateException} if the set is full (limited by couchbase document size)
     * - The producer outpaces the SDK: {@link BackpressureException}
     * - The operation had to be cancelled while on the wire or the retry strategy cancelled it instead of
     * retrying: {@link RequestCancelledException}
     * - The server is currently not able to process the request, retrying may help: {@link TemporaryFailureException}
     * - The server is out of memory: {@link CouchbaseOutOfMemoryException}
     * - Unexpected errors are caught and contained in a generic {@link CouchbaseException}.
     *
     * @param element element to be pushed into the set
     * @return true if successful, false if the element exists in set
     */
    public Observable<Boolean> add(final E element) {
        return add(element, MutationOptionBuilder.build());
    }

    /**
     * Add an element into CouchbaseSet with additional mutation options provided by
     * {@link MutationOptionBuilder}. If the underlying document for the set does not exist,
     * this operation will create a new document to back the data structure.
     * This method throws under the following conditions:
     * - {@link IllegalStateException} if the set is full (limited by couchbase document size)
     * - The producer outpaces the SDK: {@link BackpressureException}
     * - The operation had to be cancelled while on the wire or the retry strategy cancelled it instead of
     * retrying: {@link RequestCancelledException}
     * - The server is currently not able to process the request, retrying may help: {@link TemporaryFailureException}
     * - The server is out of memory: {@link CouchbaseOutOfMemoryException}
     * - Unexpected errors are caught and contained in a generic {@link CouchbaseException}.
     *
     * @param element element to be pushed into the set
     * @param mutationOptionBuilder mutation options {@link MutationOptionBuilder}
     * @return true if successful, false if the element exists in set
     */
    public Observable<Boolean> add(final E element, final MutationOptionBuilder mutationOptionBuilder) {
        final Func1<DocumentFragment<Mutation>, Boolean> mapResult = new Func1<DocumentFragment<Mutation>, Boolean>() {
            @Override
            public Boolean call(DocumentFragment<Mutation> documentFragment) {
                ResponseStatus status = documentFragment.status(0);
                if (status == ResponseStatus.SUCCESS) {
                    return true;
                } else {
                    if (status == ResponseStatus.SUBDOC_PATH_EXISTS) {
                        return false;
                    } else if (status == ResponseStatus.TOO_BIG) {
                        throw new IllegalStateException("Set full");
                    } else {
                        throw new CouchbaseException(status.toString());
                    }
                }
            }
        };
        return Observable.defer(new Func0<Observable<Boolean>>() {
            @Override
            public Observable<Boolean> call() {
                return subdocAddUnique(element, mutationOptionBuilder.cas, mutationOptionBuilder.persistTo,
                        mutationOptionBuilder.replicateTo, mutationOptionBuilder.expiry)
                        .map(mapResult);
            }
        });
    }

    private Observable<DocumentFragment<Mutation>> subdocAddUnique(final E element,
                                                                   final long cas,
                                                                   final PersistTo persistTo,
                                                                   final ReplicateTo replicateTo,
                                                                   final int expiry) {
        final Func1<JsonArrayDocument, DocumentFragment<Mutation>> mapFullDocResultToSubdoc = new
                Func1<JsonArrayDocument, DocumentFragment<Mutation>>() {
                    @Override
                    public DocumentFragment<Mutation> call(JsonArrayDocument document) {
                        List<SubdocOperationResult<Mutation>> list;
                        list = new ArrayList<SubdocOperationResult<Mutation>>();
                        list.add(SubdocOperationResult.createResult(null, Mutation.ARRAY_ADD_UNIQUE,
                                ResponseStatus.SUCCESS, element));
                        return new DocumentFragment<Mutation>(document.id(), document.cas(),
                                document.mutationToken(),
                                list);
                    }
                };

        final Func1<Throwable, Observable<? extends DocumentFragment<Mutation>>> retryIfDocExists = new
                Func1<Throwable, Observable<? extends DocumentFragment<Mutation>>>() {
                    @Override
                    public Observable<? extends DocumentFragment<Mutation>> call(Throwable throwable) {
                        if (throwable instanceof DocumentAlreadyExistsException) {
                            return subdocAddUnique(element, cas, persistTo, replicateTo, expiry);
                        } else {
                            return Observable.error(throwable);
                        }
                    }
                };

        return bucket.mutateIn(docId).arrayAddUnique("", element, false)
                .withCas(cas)
                .withDurability(persistTo, replicateTo)
                .withExpiry(expiry)
                .execute()
                .onErrorResumeNext(new Func1<Throwable, Observable<? extends DocumentFragment<Mutation>>>() {
                    @Override
                    public Observable<? extends DocumentFragment<Mutation>> call(Throwable throwable) {
                        if (throwable instanceof DocumentDoesNotExistException) {
                            return createDocument(element)
                                    .map(mapFullDocResultToSubdoc)
                                    .onErrorResumeNext(retryIfDocExists);
                        } else {
                            if (throwable instanceof MultiMutationException) {
                                ResponseStatus status = ((MultiMutationException) throwable).firstFailureStatus();
                                List<SubdocOperationResult<Mutation>> list;
                                list = new ArrayList<SubdocOperationResult<Mutation>>();
                                list.add(SubdocOperationResult.createResult(null, Mutation.ARRAY_ADD_UNIQUE,
                                        status, null));
                                return Observable.just(new DocumentFragment<Mutation>(null, 0,
                                        null,
                                        list));
                            } else {
                                return Observable.error(throwable);
                            }
                        }
                    }
                });
    }

    /**
     * Check if an element exists in CouchbaseSet.
     * This method throws under the following conditions:
     * - The producer outpaces the SDK: {@link BackpressureException}
     * - The operation had to be cancelled while on the wire or the retry strategy cancelled it instead of
     * retrying: {@link RequestCancelledException}
     * - If the underlying couchbase document does not exist: {@link DocumentDoesNotExistException}
     * - The server is currently not able to process the request, retrying may help: {@link TemporaryFailureException}
     * - The server is out of memory: {@link CouchbaseOutOfMemoryException}
     * - Unexpected errors are caught and contained in a generic {@link CouchbaseException}.
     *
     * @param element element to check for existence
     * @return true if element exists, false if the element does not exist
     */
    public Observable<Boolean> exists(final E element) {
        return Observable.defer(new Func0<Observable<Boolean>>() {
            @Override
            public Observable<Boolean> call() {
                return bucket.get(docId, JsonArrayDocument.class)
                        .map(new Func1<JsonArrayDocument, Boolean>() {
                            @Override
                            public Boolean call(JsonArrayDocument document) {
                                JsonArray jsonArray = document.content();
                                Iterator<Object> iterator = jsonArray.iterator();
                                while (iterator.hasNext()) {
                                    Object next = iterator.next();
                                    if (next == null) {
                                        if (element == next) {
                                            return true;
                                        }
                                    } else if (next.equals(element)) {
                                        return true;
                                    }
                                }
                                return false;
                            }
                        });
            }
        });
    }

    /**
     * Removes an element from CouchbaseSet.
     * This method throws under the following conditions:
     * - The producer outpaces the SDK: {@link BackpressureException}
     * - The operation had to be cancelled while on the wire or the retry strategy cancelled it instead of
     * retrying: {@link RequestCancelledException}
     * - If the underlying couchbase document does not exist: {@link DocumentDoesNotExistException}
     * - The server is currently not able to process the request, retrying may help: {@link TemporaryFailureException}
     * - The server is out of memory: {@link CouchbaseOutOfMemoryException}
     * - Unexpected errors are caught and contained in a generic {@link CouchbaseException}.
     *
     * @param element element to be removed
     * @return element removed from set (fails silently by returning the element is not found in set)
     */
    public Observable<E> remove(E element) {
        return remove(element, MutationOptionBuilder.build());
    }

    /**
     * Removes an element from CouchbaseSet with additional mutation options provided by {@link MutationOptionBuilder}.
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
     * @param element element to be removed
     * @param mutationOptionBuilder mutation options {@link MutationOptionBuilder}
     * @return element removed from set (fails silently by returning the element is not found in set)
     */
    public Observable<E> remove(final E element, final MutationOptionBuilder mutationOptionBuilder) {
        return Observable.defer(new Func0<Observable<E>>() {
            @Override
            public Observable<E> call() {
                return subdocRemove(element, mutationOptionBuilder);
            }
        });
    }

    private Observable<E> subdocRemove(final E element, final MutationOptionBuilder mutationOptionBuilder) {
        return bucket.get(docId, JsonArrayDocument.class)
                .flatMap(new Func1<JsonArrayDocument, Observable<E>>() {
                    @Override
                    public Observable<E> call(JsonArrayDocument jsonArrayDocument) {
                        Iterator iterator = jsonArrayDocument.content().iterator();
                        int ii = 0, index = -1;
                        while (iterator.hasNext()) {
                            Object next = iterator.next();
                            if (next == null) {
                                if (element == null) {
                                    index = ii;
                                    break;
                                }
                            } else if (next.equals(element)) {
                                index = ii;
                                break;
                            }
                            ii++;
                        }
                        if (index == -1) {
                            return Observable.just(element);
                        }
                        Func1<Throwable, Observable<? extends DocumentFragment<Mutation>>> handleCASMismatch = new
                                Func1<Throwable, Observable<? extends DocumentFragment<Mutation>>>() {
                                    @Override
                                    public Observable<? extends DocumentFragment<Mutation>> call(Throwable throwable) {
                                        if (throwable instanceof CASMismatchException) {
                                            return subdocRemove(element, mutationOptionBuilder).map(new Func1<E, DocumentFragment<Mutation>>() {
                                                @Override
                                                public DocumentFragment<Mutation> call(E element) {
                                                    List<SubdocOperationResult<Mutation>> list;
                                                    list = new ArrayList<SubdocOperationResult<Mutation>>();
                                                    list.add(SubdocOperationResult.createResult(null,
                                                            Mutation.DELETE,
                                                            ResponseStatus.SUCCESS, element));
                                                    return new DocumentFragment<Mutation>(null, 0, null, list);
                                                }
                                            });
                                        } else {
                                            return Observable.error(throwable);
                                        }
                                    }
                                };
                        return bucket.mutateIn(docId).remove("[" + Integer.toString(index) + "]")
                                .withCas(jsonArrayDocument.cas())
                                .withExpiry(mutationOptionBuilder.expiry)
                                .withDurability(mutationOptionBuilder.persistTo, mutationOptionBuilder.replicateTo)
                                .execute()
                                .onErrorResumeNext(handleCASMismatch)
                                .map(new Func1<DocumentFragment<Mutation>, E>() {
                                    @Override
                                    public E call(DocumentFragment<Mutation> documentFragment) {
                                        ResponseStatus status = documentFragment.status(0);
                                        if (status == ResponseStatus.SUCCESS) {
                                            return element;
                                        } else {
                                            if (status == ResponseStatus.SUBDOC_PATH_NOT_FOUND ||
                                                    status == ResponseStatus.SUBDOC_PATH_INVALID) {
                                                return element;
                                            }
                                            throw new CouchbaseException(status.toString());
                                        }
                                    }
                                });
                    }
                });
    }

    /**
     * Returns the number of elements in CouchbaseSet
     * This method throws under the following conditions:
     * - The producer outpaces the SDK: {@link BackpressureException}
     * - The operation had to be cancelled while on the wire or the retry strategy cancelled it instead of
     * retrying: {@link RequestCancelledException}
     * - If the underlying couchbase document does not exist: {@link DocumentDoesNotExistException}
     * - The server is currently not able to process the request, retrying may help: {@link TemporaryFailureException}
     * - The server is out of memory: {@link CouchbaseOutOfMemoryException}
     * - Unexpected errors are caught and contained in a generic {@link CouchbaseException}.
     *
     * @return number of elements in set
     */
    public Observable<Integer> size() {
        return new CouchbaseList<E>(bucket, docId).size();
    }
}