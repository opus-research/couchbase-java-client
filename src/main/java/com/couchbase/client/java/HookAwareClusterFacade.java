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

import com.couchbase.client.core.ClusterFacade;
import com.couchbase.client.core.message.CouchbaseRequest;
import com.couchbase.client.core.message.CouchbaseResponse;
import rx.Observable;

/**
 * A {@link ClusterFacade} wrapper which is aware of certain hooks and dispatches them
 * accordingly.
 *
 * @author Michael Nitschinger
 * @since 2.4.8
 */
public class HookAwareClusterFacade implements ClusterFacade {

    private final ClusterFacade inner;
    private final CoreHookAware hook;

    public HookAwareClusterFacade(final ClusterFacade inner, final CoreHookAware hook) {
        this.inner = inner;
        this.hook = hook;
    }

    @Override
    public <R extends CouchbaseResponse> Observable<R> send(final CouchbaseRequest request) {
        hook.beforeSend(request);
        return this.inner.send(request);
    }
}
