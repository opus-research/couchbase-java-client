/**
 * Copyright (C) 2009-2013 Couchbase, Inc.
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

package com.couchbase.client.protocol.views;

/**
 * Possible on_error arguments to view queries.
 *
 * See http://www.couchbase.com/docs/couchbase-manual-2.0/
 * couchbase-views-writing-querying-errorcontrol.html
 *
 */
public enum OnError {
  /**
   * Stop the processing of the view query when an error occurs and populate
   * the errors response with details.
   *
   */
  STOP {
    public String toString() {
      return "stop";
    }
  },

  /**
   * Continue processing the query even if errors occur, populating the errors
   * response at the end of the query response.
   *
   * This is the default if no on_error argument is supplied.
   */
  CONTINUE {
    public String toString() {
      return "continue";
    }
  }
}
