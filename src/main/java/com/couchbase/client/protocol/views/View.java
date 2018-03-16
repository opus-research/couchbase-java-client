/**
 * Copyright (C) 2009-2012 Couchbase, Inc.
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
 * Holds the decoded information from a view in the Couchbase Server.
 */
public class View {

  private final String viewName;
  private final String designDocumentName;
  private final String bucketName;
  private final boolean map;
  private final boolean reduce;
  private ViewType viewType;

  /**
   * Create a new View instance and provide all necessary details.
   *
   * The ViewType is automatically set to MAPREDUCE.
   *
   * @param bn the name of the bucket.
   * @param ddn the name of the design document.
   * @param vn the string name of the view.
   * @param m whether it has a map function or not.
   * @param r whether it has a reduce function or not.
   */
  public View(String bn, String ddn, String vn, boolean m, boolean r) {
    this(bn, ddn, vn, m, r, ViewType.MAPREDUCE);
  }

  /**
   * Create a new View instance and provide all necessary details.
   *
   * @param bn the name of the bucket.
   * @param ddn the name of the design document.
   * @param vn the string name of the view.
   * @param m whether it has a map function or not.
   * @param r whether it has a reduce function or not.
   * @param t the type of the view.
   */
  public View(String bn, String ddn, String vn, boolean m, boolean r,
    ViewType t) {
    bucketName = bn;
    designDocumentName = ddn;
    viewName = vn;
    map = m;
    reduce = r;
    viewType = t;
  }

  /**
   * Returns the bucket name of the view.
   *
   * @return the string representation of the bucket name.
   */
  public String getBucketName() {
    return bucketName;
  }

  /**
   * The name of the design document.
   *
   * @return the string representation of the design document name.
   */
  public String getDesignDocumentName() {
    return designDocumentName;
  }

  /**
   * The view name.
   *
   * @return the string representation of the view name.
   */
  public String getViewName() {
    return viewName;
  }

  /**
   * Information about the map function of the view.
   *
   * @return whether the view has a map function or not.
   */
  public boolean hasMap() {
    return map;
  }

  /**
   * Returns the information about the view type.
   *
   * @return returns the corresponding ViewType.
   */
  public ViewType getType() {
    return viewType;
  }

  /**
   * Information about the reduce function of the view.
   *
   * @return whether the view has a reduce function or not.
   */
  public boolean hasReduce() {
    return reduce;
  }

  /**
   * Returns the URI location of the view on the Couchbase Server.
   *
   * @return the URI as a string.
   */
  public String getURI() {
    String prefix = "/" + bucketName + "/_design/" + designDocumentName;
    if(viewType.equals(ViewType.MAPREDUCE)) {
      return prefix + "/_view/" + viewName;
    } else if(viewType.equals(ViewType.SPATIAL)) {
      return prefix + "/_spatial/" + viewName;
    } else {
      throw new RuntimeException("Unsupported View Type: " + viewType);
    }
  }
}
