package com.couchbase.client.java.datastructures;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import com.couchbase.client.core.CouchbaseException;
import com.couchbase.client.core.message.ResponseStatus;
import com.couchbase.client.core.message.kv.subdoc.multi.Mutation;
import com.couchbase.client.java.CouchbaseAsyncBucket;
import com.couchbase.client.java.PersistTo;
import com.couchbase.client.java.ReplicateTo;
import com.couchbase.client.java.document.JsonArrayDocument;
import com.couchbase.client.java.document.json.JsonArray;
import com.couchbase.client.java.error.CASMismatchException;
import com.couchbase.client.java.error.DocumentAlreadyExistsException;
import com.couchbase.client.java.error.DocumentDoesNotExistException;
import com.couchbase.client.java.subdoc.DocumentFragment;
import com.couchbase.client.java.subdoc.SubdocOperationResult;
import rx.Observable;
import rx.functions.Func0;
import rx.functions.Func1;

public class CouchbaseQueue<E> {

    private final String docId;
    private final CouchbaseAsyncBucket bucket;

    public CouchbaseQueue(CouchbaseAsyncBucket bucket, String docId) {
        this.bucket = bucket;
        this.docId = docId;
    }

    private Observable<JsonArrayDocument> createDocument(E element) {
        return bucket.upsert(JsonArrayDocument.create(docId, JsonArray.create().add(element)));
    }

    public Observable<Boolean> add(final E element) {
        return add(element, MutationOptionBuilder.build());
    }

    public Observable<Boolean> add(final E element, final MutationOptionBuilder optionBuilder) {
        return Observable.defer(new Func0<Observable<Boolean>>() {
            @Override
            public Observable<Boolean> call() {
                return subdocAddLast(element, optionBuilder.cas, optionBuilder.persistTo, optionBuilder.replicateTo, optionBuilder.expiry)
                        .map(new Func1<DocumentFragment<Mutation>, Boolean>() {
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
                        });
            }
        });
    }

    private Observable<DocumentFragment<Mutation>> subdocAddLast(final E element,
                                                                 final long cas,
                                                                 final PersistTo persistTo,
                                                                 final ReplicateTo replicateTo,
                                                                 final int expiry) {
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
                                    .map(new Func1<JsonArrayDocument, DocumentFragment<Mutation>>() {
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
                                    }).onErrorResumeNext(new Func1<Throwable, Observable<? extends DocumentFragment<Mutation>>>() {
                                        @Override
                                        public Observable<? extends DocumentFragment<Mutation>> call(Throwable throwable) {
                                            if (throwable instanceof DocumentAlreadyExistsException) {
                                                return subdocAddLast(element, cas, persistTo, replicateTo, expiry);
                                            } else {
                                                return Observable.error(throwable);
                                            }
                                        }
                                    });
                        } else {
                            return Observable.error(throwable);
                        }
                    }
                });
    }

    public Observable<E> remove() {
        return remove(MutationOptionBuilder.build());
    }

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
                        return bucket.mutateIn(docId).remove("[" + Integer.toString(-1) + "]")
                                .withCas(jsonArrayDocument.cas())
                                .withExpiry(mutationOptionBuilder.expiry)
                                .withDurability(mutationOptionBuilder.persistTo, mutationOptionBuilder.replicateTo)
                                .execute()
                                .onErrorResumeNext(new Func1<Throwable, Observable<? extends DocumentFragment<Mutation>>>() {
                                    @Override
                                    public Observable<? extends DocumentFragment<Mutation>> call(Throwable throwable) {
                                        if (throwable instanceof CASMismatchException) {
                                            return subdocRemove(mutationOptionBuilder).map(new Func1<E, DocumentFragment<Mutation>>() {
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
                                }).map(new Func1<DocumentFragment<Mutation>, E>() {
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
                                            if (status == ResponseStatus.SUBDOC_PATH_NOT_FOUND || status == ResponseStatus.SUBDOC_PATH_INVALID) {
                                                throw new NoSuchElementException();
                                            }
                                            throw new CouchbaseException(status.toString());
                                        }
                                    }
                                });
                    }
                });
    }

    public Observable<Integer> size() {
        return new CouchbaseList<E>(bucket, docId).size();
    }
}
