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

import com.couchbase.client.core.BackpressureException;
import com.couchbase.client.core.CouchbaseException;
import com.couchbase.client.core.RequestCancelledException;
import com.couchbase.client.core.annotations.InterfaceAudience;
import com.couchbase.client.core.annotations.InterfaceStability;
import com.couchbase.client.java.*;
import com.couchbase.client.java.document.JsonArrayDocument;
import com.couchbase.client.java.document.json.JsonArray;
import com.couchbase.client.java.error.*;

/**
 * CouchbaseSet data structure backed by JsonArrayDocument and has set collection like interface supporting operations
 * that can be executed synchronously against a Couchbase Server bucket.
 *
 * @param <E> Type of element
 * @author Subhashni Balakrishnan
 */
@InterfaceStability.Experimental
@InterfaceAudience.Public
public class CouchbaseSet<E> {

	private final String docId;
	private final Bucket bucket;
	private final Class<E> type;

	/**
	 * Create {@link Bucket Couchbase-backed} CouchbaseSet, backed by the document identified by <code>id</code>
	 * in <code>bucket</code>. Note that if the document with the data structure already exists,
	 * its content will be used as initial content.
	 *
	 * This method throws under the following conditions:
	 * - The producer outpaces the SDK: {@link BackpressureException}
	 * - The operation had to be cancelled while on the wire or the retry strategy cancelled it instead of
	 * retrying: {@link RequestCancelledException}
	 * - If the underlying couchbase document already exists: {@link DocumentAlreadyExistsException}
	 * - The server is currently not able to process the request, retrying may help: {@link TemporaryFailureException}
	 * - The server is out of memory: {@link CouchbaseOutOfMemoryException}
	 * - Unexpected errors are caught and contained in a generic {@link CouchbaseException}.
	 *
	 * @param bucket the {@link Bucket} through which to interact with the document.
	 * @param docId the id of the Couchbase document to back the list.
	 * @param type generic class type
	 */
	public CouchbaseSet(Bucket bucket, String docId, Class<E> type) {
		this.bucket = bucket;
		this.docId = docId;
		this.type = type;
		if (!bucket.exists(docId)) {
			bucket.insert(JsonArrayDocument.create(docId, JsonArray.create()));
		}
	}

	/**
	 * Add an element into CouchbaseSet
	 *
	 * This method throws under the following conditions:
	 * - The producer outpaces the SDK: {@link BackpressureException}
	 * - The operation had to be cancelled while on the wire or the retry strategy cancelled it instead of
	 * retrying: {@link RequestCancelledException}
	 * - If the underlying couchbase document does not exist: {@link DocumentDoesNotExistException}
	 * - The server is currently not able to process the request, retrying may help: {@link TemporaryFailureException}
	 * - The server is out of memory: {@link CouchbaseOutOfMemoryException}
	 * - Unexpected errors are caught and contained in a generic {@link CouchbaseException}.
	 *
	 * @param element element to be pushed into the set
	 * @return true if successful, false if the element exists in set
	 */
	public Boolean add(final E element) {
		return this.bucket.setAdd(this.docId, element);
	}

	/**
	 * Add an element into CouchbaseSet with additional mutation options provided by {@link MutationOptionBuilder}
	 *
	 * This method throws under the following conditions:
	 * - The producer outpaces the SDK: {@link BackpressureException}
	 * - The operation had to be cancelled while on the wire or the retry strategy cancelled it instead of
	 * retrying: {@link RequestCancelledException}
	 * - If the underlying couchbase document does not exist: {@link DocumentDoesNotExistException}
	 * - The server is currently not able to process the request, retrying may help: {@link TemporaryFailureException}
	 * - The server is out of memory: {@link CouchbaseOutOfMemoryException}
	 * - Unexpected errors are caught and contained in a generic {@link CouchbaseException}.
	 *
	 * @param element element to be pushed into the set
	 * @param mutationOptionBuilder mutation options {@link MutationOptionBuilder}
	 * @return true if successful, false if the element exists in set
	 */
	public Boolean add(final E element, MutationOptionBuilder mutationOptionBuilder) {
		return this.bucket.setAdd(this.docId, element, mutationOptionBuilder);
	}

	/**
	 * Check if an element exists in CouchbaseSet
	 *
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
	public Boolean exists(final E element) {
		return this.bucket.setContains(this.docId, element);
	}

	/**
	 * Removes an element from CouchbaseSet
	 *
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
	public E remove(E element) {
		return this.bucket.setRemove(this.docId, element);
	}

	/**
	 * Removes an element from CouchbaseSet with additional mutation options provided by {@link MutationOptionBuilder}
	 *
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
	public E remove(final E element, final MutationOptionBuilder mutationOptionBuilder) {
		return this.bucket.setRemove(this.docId, element, mutationOptionBuilder);
	}

	/**
	 * Returns the number of elements in CouchbaseSet
	 *
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
	public int size() {
		return this.bucket.setSize(this.docId);
	}
}