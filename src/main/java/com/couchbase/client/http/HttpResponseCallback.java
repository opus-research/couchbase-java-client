package com.couchbase.client.http;

import com.couchbase.client.ViewConnection;
import com.couchbase.client.protocol.views.HttpOperation;
import net.spy.memcached.compat.SpyObject;

import org.apache.http.HttpResponse;
import org.apache.http.concurrent.FutureCallback;
import org.apache.http.entity.BufferedHttpEntity;
import org.apache.http.util.EntityUtils;

import java.io.IOException;


public class HttpResponseCallback extends SpyObject
  implements FutureCallback<HttpResponse> {

  private final HttpOperation op;
  private final ViewConnection vconn;

  public HttpResponseCallback(HttpOperation op, ViewConnection vconn) {
    this.op = op;
    this.vconn = vconn;
  }

  @Override
  public void completed(final HttpResponse response) {
    try {
      response.setEntity(new BufferedHttpEntity(response.getEntity()));
    } catch(IOException ex) {
      throw new RuntimeException("Could not convert HttpEntity content.");
    }
    int statusCode = response.getStatusLine().getStatusCode();
    boolean shouldRetry = shouldRetry(statusCode, response);
    if (shouldRetry) {
      if(!op.isTimedOut() && !op.isCancelled()) {
        getLogger().info("Retrying HTTP operation Request: "
          + op.getRequest().getRequestLine() + ", Response: "
          + response.getStatusLine());
        vconn.addOp(op);
      }
    } else {
      op.handleResponse(response);
    }
  }

  @Override
  public void failed(Exception e) {
    getLogger().debug("View Operation " + op.getRequest().getRequestLine()
      + " failed because of: ", e);
    op.cancel();
  }

  @Override
  public void cancelled() {
    getLogger().debug("View Operation " + op.getRequest().getRequestLine()
      + " got cancelled.");
    op.cancel();
  }

  private static boolean shouldRetry(int statusCode, HttpResponse response) {
    switch(statusCode) {
      case 200:
        return false;
      case 404:
        return analyse404Response(response);
      case 500:
        return analyse500Response(response);
      case 300:
      case 301:
      case 302:
      case 303:
      case 307:
      case 401:
      case 408:
      case 409:
      case 412:
      case 416:
      case 417:
      case 501:
      case 502:
      case 503:
      case 504:
        return true;
      default:
        return false;
    }
  }

  private static boolean analyse404Response(HttpResponse response) {
    try {
      String body = EntityUtils.toString(response.getEntity());
      // Indicates a Not Found Design Document
      if(body.contains("not_found")
        && (body.contains("missing") || body.contains("deleted"))) {
        return false;
      }
    } catch(IOException ex) {
      return false;
    }
    return true;
  }

  private static boolean analyse500Response(HttpResponse response) {
    try {
      String body = EntityUtils.toString(response.getEntity());
      // Indicates a Not Found Design Document
      if(body.contains("error")
        && body.contains(("{not_found, missing_named_view}"))) {
        return false;
      }
    } catch(IOException ex) {
      return false;
    }
    return true;
  }

}
