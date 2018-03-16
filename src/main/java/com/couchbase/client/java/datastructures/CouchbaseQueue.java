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
 * Couchbase Queue data structure backed by JsonArrayDocument.
 * It has queue collection like interface supporting operations that can be executed asynchronously
 * against a Couchbase Server bucket.
 *
 * @param <E> Type of element
 * @author subhashni
 */
@InterfaceStability.Experimental
@InterfaceAudience.Public
public class CouchbaseQueue<E> {

    private final String docId;
    private final CouchbaseAsyncBucket bucket;

    /**
     * Create {@link Bucket Couchbase-backed} CouchbaseQueue, backed by the document identified by <code>id</code>
     * in <code>bucket</code>. Note that if the document with the data structure already exists,
     * its content will be used as initial content.
     *
     * @param bucket the {@link Bucket} through which to interact with the document.
     * @param docId the id of the Couchbase document to back the list.
     */
    public CouchbaseQueue(CouchbaseAsyncBucket bucket, String docId) {
        this.bucket = bucket;
        this.docId = docId;
    }

    private Observable<JsonArrayDocument> createDocument(E element) {
        return bucket.upsert(JsonArrayDocument.create(docId, JsonArray.create().add(element)));
    }

    /**
     * Add an element into CouchbaseQueue. If the underlying document for the queue does not exist,
     * this operation will create a new document to back the data structure.
     * This method throws under the following conditions:
     * - {@link IllegalStateException} if the queue is full (limited by couchbase document size)
     * - The producer outpaces the SDK: {@link BackpressureException}
     * - The operation had to be cancelled while on the wire or the retry strategy cancelled it instead of
     * retrying: {@link RequestCancelledException}
     * - The server is currently not able to process the request, retrying may help: {@link TemporaryFailureException}
     * - The server is out of memory: {@link CouchbaseOutOfMemoryException}
     * - Unexpected errors are caught and contained in a generic {@link CouchbaseException}.
     *
     * @param element element to be pushed into the queue
     * @return true if successful
     */
    public Observable<Boolean> add(final E element) {
        return add(element, MutationOptionBuilder.build());
    }

    /**
     * Add an element into CouchbaseQueue with additional mutation options provided by {@link MutationOptionBuilder}.
     * If the underlying document for the queue does not exist, this operation will create a new document to back
     * the data structure.
     * This method throws under the following conditions:
     * - {@link IllegalStateException} if the queue is full (limited by couchbase document size)
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
     * @param element element to be pushed into the queue
     * @param mutationOptionBuilder mutation options {@link MutationOptionBuilder}
     * @return true if successful
     */
    public Observable<Boolean> add(final E element, final MutationOptionBuilder mutationOptionBuilder) {
        final Func1<DocumentFragment<Mutation>, Boolean> mapResult = new
                Func1<DocumentFragment<Mutation>, Boolean>() {
                    @Override
                    public Boolean call(DocumentFragment<Mutation> documentFragment) {
                        ResponseStatus status = documentFragment.status(0);
                        if (status == ResponseStatus.SUCCESS) {
                            return true;
                        } else {
                            if (status == ResponseStatus.TOO_BIG) {
                                throw new IllegalStateException("Queue full");
                            }
                            return false;
                        }
                    }
                };
        return Observable.defer(new Func0<Observable<Boolean>>() {
            @Override
            public Observable<Boolean> call() {
                return subdocAddLast(element, mutationOptionBuilder.cas, mutationOptionBuilder.persistTo,
                        mutationOptionBuilder.replicateTo, mutationOptionBuilder.expiry)
                        .map(mapResult);
            }
        });
    }

    private Observable<DocumentFragment<Mutation>> subdocAddLast(final E element,
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
                        list.add(SubdocOperationResult.createResult(null,
                                Mutation.ARRAY_PUSH_LAST,
                                ResponseStatus.SUCCESS, element));
                        return new DocumentFragment<Mutation>(document.id(),
                                document.cas(),
                                document.mutationToken(),
                                list);
                    }
                };

        final Func1<Throwable, Observable<? extends DocumentFragment<Mutation>>> retryIfDocExists = new
                Func1<Throwable, Observable<? extends DocumentFragment<Mutation>>>() {
                    @Override
                    public Observable<? extends DocumentFragment<Mutation>> call(Throwable throwable) {
                        if (throwable instanceof DocumentAlreadyExistsException) {
                            return subdocAddLast(element, cas, persistTo, replicateTo, expiry);
                        } else {
                            return Observable.error(throwable);
                        }
                    }
                };

        return bucket.mutateIn(docId).arrayAppend("", element, false)
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
                                list.add(SubdocOperationResult.createResult(null, Mutation.ARRAY_PUSH_LAST,
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
     * Removes the first element from CouchbaseQueue.
     * This method throws under the following conditions:
     * - {@link IllegalStateException} if the queue is empty
     * - The producer outpaces the SDK: {@link BackpressureException}
     * - The operation had to be cancelled while on the wire or the retry strategy cancelled it instead of
     * retrying: {@link RequestCancelledException}
     * - If the underlying couchbase document does not exist: {@link DocumentDoesNotExistException}
     * - The server is currently not able to process the request, retrying may help: {@link TemporaryFailureException}
     * - The server is out of memory: {@link CouchbaseOutOfMemoryException}
     * - Unexpected errors are caught and contained in a generic {@link CouchbaseException}.
     *
     * @return element removed from queue
     */
    public Observable<E> remove() {
        return remove(MutationOptionBuilder.build());
    }

    /**
     * Removes the first element from CouchbaseQueue with additional mutation options provided by
     * {@link MutationOptionBuilder}.
     * This method throws under the following conditions:
     * - {@link IllegalStateException} if the queue is empty
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
     * @return element removed from queue
     */
    public Observable<E> remove(final MutationOptionBuilder mutationOptionBuilder) {
        return Observable.defer(new Func0<Observable<E>>() {
            @Override
            public Observable<E> call() {
                return subdocRemove(mutationOptionBuilder);
            }
        });
    }

    private Observable<E> subdocRemove(final MutationOptionBuilder mutationOptionBuilder) {
        return bucket.get(docId, JsonArrayDocument.class)
                .flatMap(new Func1<JsonArrayDocument, Observable<E>>() {
                    @Override
                    public Observable<E> call(JsonArrayDocument jsonArrayDocument) {
                        int size = jsonArrayDocument.content().size();
                        final Object val;
                        if (size > 0) {
                            val = jsonArrayDocument.content().get(size - 1);
                        } else {
                            throw new IllegalStateException("Queue empty");
                        }
                        if (mutationOptionBuilder.cas != 0 && jsonArrayDocument.cas() != mutationOptionBuilder.cas) {
                            throw new CASMismatchException();
                        }
                        Func1<Throwable, Observable<? extends DocumentFragment<Mutation>>> handleCASMismatch = new
                                Func1<Throwable, Observable<? extends DocumentFragment<Mutation>>>() {
                                    @Override
                                    public Observable<? extends DocumentFragment<Mutation>> call(Throwable throwable) {
                                        if (throwable instanceof CASMismatchException) {
                                            return subdocRemove(mutationOptionBuilder).map(new Func1<E, DocumentFragment<Mutation>>() {
                                                @Override
                                                public DocumentFragment<Mutation> call(E element) {
                                                    List<SubdocOperationResult<Mutation>> list;
                                                    list = new ArrayList<SubdocOperationResult<Mutation>>();
                                                    list.add(SubdocOperationResult.createResult(null, Mutation.DELETE,
                                                            ResponseStatus.SUCCESS, element));
                                                    return new DocumentFragment<Mutation>(null, 0, null, list);
                                                }
                                            });
                                        } else {
                                            return Observable.error(throwable);
                                        }
                                    }
                                };
                        return bucket.mutateIn(docId).remove("[" + Integer.toString(-1) + "]")
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
                                            if (documentFragment.content(0) != null) {
                                                return (E) documentFragment.content(0);
                                            } else {
                                                return (E) val;
                                            }
                                        } else {
                                            if (status == ResponseStatus.SUBDOC_PATH_NOT_FOUND) {
                                                throw new NoSuchElementException();
                                            }
                                            throw new CouchbaseException(status.toString());
                                        }
                                    }
                                });
                    }
                });
    }

    /**
     * Returns the number of elements in CouchbaseQueue
     * This method throws under the following conditions:
     * - The producer outpaces the SDK: {@link BackpressureException}
     * - The operation had to be cancelled while on the wire or the retry strategy cancelled it instead of
     * retrying: {@link RequestCancelledException}
     * - If the underlying couchbase document does not exist: {@link DocumentDoesNotExistException}
     * - The server is currently not able to process the request, retrying may help: {@link TemporaryFailureException}
     * - The server is out of memory: {@link CouchbaseOutOfMemoryException}
     * - Unexpected errors are caught and contained in a generic {@link CouchbaseException}.
     *
     * @return number of elements
     */
    public Observable<Integer> size() {
        return new CouchbaseList<E>(bucket, docId).size();
    }
}
