/**
 * Copyright (C) 2014 Couchbase, Inc.
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
package com.couchbase.client.java.error;

import com.couchbase.client.core.CouchbaseException;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * @author Sergey Avseyev
 * @since 2.0
 */
public class ViewQueryException extends CouchbaseException implements Iterable<ViewQueryException.Error> {
    private List<Error> errors = new ArrayList<>();

    public ViewQueryException() {
        super();
    }

    public ViewQueryException(String message) {
        super(message);
    }

    public ViewQueryException(String message, Throwable cause) {
        super(message, cause);
    }

    public ViewQueryException(Throwable cause) {
        super(cause);
    }

    public ViewQueryException(String error, String info, Throwable cause) {
        super(error + ": " + info, cause);
        errors.add(new Error(error, info));
    }

    public ViewQueryException(String error, String info) {
        super(error + ": " + info);
        addError(error, info);
    }

    public List<Error> errors() {
        return errors;
    }

    public void addError(final String error, final String info) {
        errors.add(new Error(error, info));
    }

    @Override
    public Iterator<Error> iterator() {
        return errors.iterator();
    }

    public class Error {
        private final String error;
        private final String info;

        public Error(String error, String info) {
            this.error = error;
            this.info = info;
        }

        public String error() {
            return error;
        }

        public String info() {
            return info;
        }
    }
}
