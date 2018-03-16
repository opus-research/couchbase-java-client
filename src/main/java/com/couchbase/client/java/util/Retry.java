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
package com.couchbase.client.java.util;

import java.util.concurrent.TimeUnit;

import com.couchbase.client.core.lang.Tuple;
import com.couchbase.client.core.lang.Tuple2;
import com.couchbase.client.core.time.Delay;
import com.couchbase.client.core.time.ExponentialDelay;
import com.couchbase.client.java.error.CannotRetryException;
import rx.Observable;
import rx.functions.Func1;
import rx.functions.Func2;

/**
 * Utility methods to deal with retrying {@link Observable}s.
 *
 * @author Simon Baslé
 * @since 1.1
 */
public class Retry {

    /**
     * Wrap an {@link Observable} so that it will retry on all errors for a maximum number of times.
     * The retry is almost immediate (1ms delay).
     *
     * @param source the {@link Observable} to wrap.
     * @param maxAttempts the maximum number of times to attempt a retry.
     * @param <T> the type of items emitted by the source Observable.
     * @return the wrapped retrying Observable.
     */
    public static <T> Observable<T> wrapForRetry(Observable<T> source, int maxAttempts) {
        return wrapForRetry(source, new RetryWithDelayHandler(maxAttempts, Delay.fixed(1, TimeUnit.MILLISECONDS)));
    }

    /**
     * Wrap an {@link Observable} so that it will retry on all errors. The retry will occur for a maximum number of
     * attempts and with a provided {@link Delay} between each attempt.
     *
     * @param source the {@link Observable} to wrap.
     * @param maxAttempts the maximum number of times to attempt a retry.
     * @param retryDelay the {@link Delay} between each attempt.
     * @param <T> the type of items emitted by the source Observable.
     * @return the wrapped retrying Observable.
     */
    public static <T> Observable<T> wrapForRetry(Observable<T> source, int maxAttempts, Delay retryDelay) {
        return wrapForRetry(source, new RetryWithDelayHandler(maxAttempts, retryDelay));
    }

    /**
     * Wrap an {@link Observable} so that it will retry on some errors. The retry will occur for a maximum number of
     * attempts and with a provided {@link Delay} between each attempt represented by the {@link RetryWithDelayHandler},
     * which can also filter on errors and stop the retry cycle for certain type of errors.
     *
     * @param source the {@link Observable} to wrap.
     * @param handler the {@link RetryWithDelayHandler}, describes maximum number of attempts, delay and fatal errors.
     * @param <T> the type of items emitted by the source Observable.
     * @return the wrapped retrying Observable.
     */
    public static <T> Observable<T> wrapForRetry(Observable<T> source, final RetryWithDelayHandler handler) {
        return source
                .retryWhen(
                        new Func1<Observable<? extends Throwable>, Observable<?>>() {
                            @Override
                            public Observable<?> call(Observable<? extends Throwable> errors) {
                                return errorsWithAttempts(errors, handler.maxAttempts).flatMap(handler);
                            }
                        });
    }


    /**
     * Internal utility method to combine errors in an observable with their attempt number.
     *
     * @param errors the errors.
     * @param maxAttempts the maximum of combinations to make.
     * @return an Observable that combines the index/attempt number of each error with its error in a {@link Tuple2}.
     */
    protected static Observable<Tuple2<Integer, Throwable>> errorsWithAttempts(Observable<? extends Throwable> errors,
            final int maxAttempts) {
        return errors.zipWith(
                Observable.range(0, maxAttempts),
                new Func2<Throwable, Integer, Tuple2<Integer, Throwable>>() {
                    @Override
                    public Tuple2<Integer, Throwable> call(Throwable error, Integer attempt) {
                        return Tuple.create(attempt, error);
                    }
                }
        );
    }

    /**
     * A class that allows to handle and customize retry behavior of an {@link Observable} wrapped
     * via {@link #wrapForRetry(Observable, RetryWithDelayHandler) wrapForRetry}.
     */
    public static class RetryWithDelayHandler implements Func1<Tuple2<Integer, Throwable>, Observable<?>> {

        public final int maxAttempts;
        public final Delay retryDelay;
        private final Func1<Throwable, Boolean> stoppingErrorFilter;

        /**
         * Construct a {@link RetryWithDelayHandler retry handler} that will retry on all errors.
         *
         * @param maxAttempts the maximum number of retries before a {@link CannotRetryException} is thrown.
         * @param retryDelay the {@link Delay} to apply between each retry (can grow,
         *  eg. by using {@link ExponentialDelay}).
         */
        public RetryWithDelayHandler(int maxAttempts, Delay retryDelay) {
            this(maxAttempts, retryDelay, null);
        }

        /**
         * Construct a {@link RetryWithDelayHandler retry handler} that will retry on most errors but will stop on specific errors.
         *
         * @param maxAttempts the maximum number of retries before a {@link CannotRetryException} is thrown.
         * @param retryDelay the {@link Delay} to apply between each retry (can grow,
         *  eg. by using {@link ExponentialDelay}).
         * @param stoppingErrorFilter a predicate that determine if an error must stop the retry cycle (when true),
         *  in which case a {@link CannotRetryException} is thrown.
         */
        public RetryWithDelayHandler(int maxAttempts, Delay retryDelay, Func1<Throwable, Boolean> stoppingErrorFilter) {
            this.maxAttempts = maxAttempts;
            this.retryDelay = retryDelay;
            this.stoppingErrorFilter = stoppingErrorFilter;
        }

        @Override
        public Observable<?> call(Tuple2<Integer, Throwable> attemptError) {
            long attempt = attemptError.value1();
            Throwable error = attemptError.value2();

            if (attempt >= maxAttempts) {
                return Observable.error(new CannotRetryException("Max attempt " + maxAttempts + " reached"));
            } else if (stoppingErrorFilter != null && stoppingErrorFilter.call(error) == Boolean.TRUE) {
                return Observable.error(new CannotRetryException("Cannot retry error on attempt #" + attempt, error));
            } else {
                long delay = retryDelay.calculate(attempt);
                TimeUnit unit = retryDelay.unit();
                return Observable.timer(delay, unit);
            }
        }
    }
}
