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

package com.couchbase.client.vbucket;

import com.couchbase.client.vbucket.config.Bucket;

import java.util.Observable;
import java.util.Observer;
import java.util.logging.Logger;

/**
 * An implementation of the observer for calling reconfigure.
 */
public class ReconfigurableObserver implements Observer {
  private final Reconfigurable rec;
  private static final Logger LOGGER = Logger.getLogger(
         ReconfigurableObserver.class.getName());

  public ReconfigurableObserver(Reconfigurable rec) {
    this.rec = rec;
  }

  /**
   * Delegates update to the reconfigurable passed in the constructor.
   *
   * @param o
   * @param arg
   */
  public void update(Observable o, Object arg) {
    LOGGER.finest("Received an update, notifying reconfigurables about a "
      + arg.getClass().getName() + arg.toString());
    LOGGER.finest("It says it is " + ((Bucket)arg).getName()
      + " and it's talking to " + ((Bucket)arg).getStreamingURI());
    rec.reconfigure((Bucket) arg);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    ReconfigurableObserver that = (ReconfigurableObserver) o;

    if (!rec.equals(that.rec)) {
      return false;
    }
    return true;
  }

  @Override
  public int hashCode() {
    return rec.hashCode();
  }
}
