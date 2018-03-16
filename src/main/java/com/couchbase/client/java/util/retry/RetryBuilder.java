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
package com.couchbase.client.java.util.retry;

import java.util.Arrays;
import java.util.List;

import com.couchbase.client.core.annotations.InterfaceAudience;
import com.couchbase.client.core.annotations.InterfaceStability;
import com.couchbase.client.core.time.Delay;
import rx.Scheduler;
import rx.functions.Func1;

/**
 * Builder for {@link RetryWhenFunction}. Start with {@link #retryOnce()} or {@link #retryMax(int)} factory methods.
 *
 * By default, without calling additional methods it will retry the specified number of times, with a constant delay
 * (see {@link Retry#DEFAULT_DELAY}) and on all errors.
 *
 * @author Simon Baslé
 * @since 2.1
 */
@InterfaceStability.Experimental
@InterfaceAudience.Public
public class RetryBuilder {

    private int maxAttempts;

    private Delay delay;

    private List<Class<? extends Throwable>> errorsStoppingRetry;

    private boolean inverse;

    private Scheduler scheduler;

    private RetryBuilder() {
        this.maxAttempts = 1; //one attempt
        this.delay = Retry.DEFAULT_DELAY; //constant 1ms
        this.errorsStoppingRetry = null; //retry on any error
        this.inverse = false; //list above is indeed list of errors that can stop retry (none)
        this.scheduler = null; //operate on default Scheduler for timer delay
    }

    /** Make only one retry attempt */
    public static RetryBuilder retryOnce() {
        return new RetryBuilder();
    }

    /** Make maximum maxAttempts retry attempts */
    public static RetryBuilder retryMax(int maxAttempts) {
        RetryBuilder builder = new RetryBuilder();
        builder.maxAttempts = maxAttempts;
        return builder;
    }

    /** Only errors that are NOT instanceOf the specified types will trigger a retry */
    public RetryBuilder onlyWhenNot(Class<? extends Throwable>... types) {
        this.errorsStoppingRetry = Arrays.asList(types);
        this.inverse = false;
        return this;
    }

    /** Only errors that are instanceOf the specified types will trigger a retry */
    public RetryBuilder onlyWhen(Class<? extends Throwable>... types) {
        this.errorsStoppingRetry = Arrays.asList(types);
        this.inverse = true;
        return this;
    }

    /** Customize the retry {@link Delay} */
    public RetryBuilder withDelay(Delay delay) {
        this.delay = delay;
        return this;
    }

    /** Set the {@link Scheduler} on which the delay is waited **/
    public RetryBuilder delayOn(Scheduler scheduler) {
        this.scheduler = scheduler;
        return this;
    }

    /** Construct the resulting {@link RetryWhenFunction} */
    public RetryWhenFunction build() {
        RetryWithDelayHandler handler;
        ShouldStopOnError filter;
        if (errorsStoppingRetry == null || errorsStoppingRetry.isEmpty()) {
            //always retry on any error
            filter = null;
        } else {
            filter = new ShouldStopOnError(errorsStoppingRetry, inverse);
        }

        if (scheduler == null) {
            handler = new RetryWithDelayHandler(maxAttempts, delay, filter);
        } else {
            handler = new RetryWithDelayHandler(maxAttempts, delay, filter, scheduler);
        }
        return new RetryWhenFunction(handler);
    }

    protected static class ShouldStopOnError implements Func1<Throwable, Boolean> {

        private final List<Class<? extends Throwable>> errorsStoppingRetry;
        private final boolean inverse;

        public ShouldStopOnError(List<Class<? extends Throwable>> filterErrorList, boolean inverse) {
            this.errorsStoppingRetry = filterErrorList;
            this.inverse = inverse;
        }

        @Override
        public Boolean call(Throwable o) {
            //if inverse is false, only errors in the list should prevent retry
            //if inverse is true, all errors except ones in list should prevent retry
            for (Class<? extends Throwable> aClass : errorsStoppingRetry) {
                if (aClass.isInstance(o)) {
                    return !inverse;
                }
            }
            return inverse;
        }
    }
}
