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
import com.couchbase.client.java.view.ViewResult;

/**
 * @author Sergey Avseyev
 * @since 2.0
 */
public class ViewQueryException extends CouchbaseException {
    private String error;
    private String reason;

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

    public ViewQueryException(String error, String reason, Throwable cause) {
        super(error + ": " + reason, cause);
        this.error = error;
        this.reason = reason;
    }

    public ViewQueryException(String error, String reason) {
        super(error + ": " + reason);
        this.error = error;
        this.reason = reason;
    }

    public String getReason() {
        return reason;
    }

    public String getError() {
        return error;
    }
}
