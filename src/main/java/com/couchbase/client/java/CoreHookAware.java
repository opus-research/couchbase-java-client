/*
 * Copyright (c) 2017 Couchbase, Inc.
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
package com.couchbase.client.java;

import com.couchbase.client.core.annotations.InterfaceAudience;
import com.couchbase.client.core.annotations.InterfaceStability;
import com.couchbase.client.core.message.CouchbaseRequest;

/**
 * Marker interface which can be implemented to consume certain hooks around the core
 * cluster facade.
 *
 * @author Michael Nitschinger
 * @since 2.4.8
 */
@InterfaceAudience.Public
@InterfaceStability.Experimental
public interface CoreHookAware {

    /**
     * Will be called right before the request is shot off into the request RingBuffer
     * giving the implementor a last chance to modify/listen on the request before it
     * goes into the realm of the core layer.
     *
     * @param request the request which will be written into the ringbuffer afterwards.
     */
    void beforeSend(CouchbaseRequest request);

}
