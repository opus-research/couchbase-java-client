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

package com.couchbase.client;

import com.couchbase.client.protocol.views.HttpOperation;
import com.couchbase.client.vbucket.Reconfigurable;

import java.io.IOException;
import java.util.List;

/**
 * Defines the contract of a {@link ViewConnection}.
 *
 * A {@link ViewConnection} is maintains the connected view nodes, accepts
 * and manages view requests. It is designed to be self contained and properly
 * respond to changes that arrive during rebalance.
 */
public interface ViewConnection extends Reconfigurable {

  /**
   * Queue a {@link HttpOperation} for execution.
   *
   * @param op the operation to queue.
   */
  void addOp(final HttpOperation op);

  /**
   * Returns a list of connected nodes.
   *
   * // TODO: fix the ViewNode Interface properly.
   *
   * @return a list of currently connected nodes.
   */
  List<DefaultViewNode> getConnectedNodes();

  /**
   * Shut down the {@link ViewConnection}.
   *
   * @return true if correctly shut down.
   * @throws IOException if something happened during shutdown.
   */
  boolean shutdown() throws IOException;

  /**
   * Returns the state of the {@link ViewConnection}.
   */
  void checkState();

}
