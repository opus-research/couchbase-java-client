package com.couchbase.client.java.analytics;

import com.couchbase.client.core.ClusterFacade;
import com.couchbase.client.core.message.analytics.GenericAnalyticsRequest;
import com.couchbase.client.core.message.analytics.GenericAnalyticsResponse;
import com.couchbase.client.deps.io.netty.buffer.ByteBuf;
import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.client.java.error.TranscodingException;
import com.couchbase.client.java.transcoder.TranscoderUtils;
import rx.Observable;
import rx.functions.Func0;
import rx.functions.Func1;

import java.util.Arrays;

import static com.couchbase.client.java.CouchbaseAsyncBucket.JSON_OBJECT_TRANSCODER;

public class AnalyticsQueryExecutor {

    private final ClusterFacade core;
    private final String bucket;
    private final String password;

    public AnalyticsQueryExecutor(ClusterFacade core, String bucket, String password) {
        this.core = core;
        this.bucket = bucket;
        this.password = password;
    }

    public Observable<AsyncAnalyticsQueryResult> execute(final AnalyticsQuery query) {
        return Observable.defer(new Func0<Observable<GenericAnalyticsResponse>>() {
            @Override
            public Observable<GenericAnalyticsResponse> call() {
                return core.send(GenericAnalyticsRequest.jsonQuery(query.query().toString(), bucket, password));
            }
        }).flatMap(new Func1<GenericAnalyticsResponse, Observable<AsyncAnalyticsQueryResult>>() {
            @Override
            public Observable<AsyncAnalyticsQueryResult> call(final GenericAnalyticsResponse response) {
                final Observable<AsyncAnalyticsQueryRow> rows = response.rows().map(new Func1<ByteBuf, AsyncAnalyticsQueryRow>() {
                    @Override
                    public AsyncAnalyticsQueryRow call(ByteBuf byteBuf) {
                        try {
                            TranscoderUtils.ByteBufToArray rawData = TranscoderUtils.byteBufToByteArray(byteBuf);
                            byte[] copy = Arrays.copyOfRange(rawData.byteArray, rawData.offset, rawData.offset + rawData.length);
                            return new DefaultAsyncAnalyticsQueryRow(copy);
                        } catch (Exception e) {
                            throw new TranscodingException("Could not decode N1QL Query Row.", e);
                        } finally {
                            byteBuf.release();
                        }
                    }
                });
                final Observable<Object> signature = response.signature().map(new Func1<ByteBuf, Object>() {
                    @Override
                    public Object call(ByteBuf byteBuf) {
                        try {
                            return JSON_OBJECT_TRANSCODER.byteBufJsonValueToObject(byteBuf);
                        } catch (Exception e) {
                            throw new TranscodingException("Could not decode N1QL Query Signature", e);
                        } finally {
                            byteBuf.release();
                        }
                    }
                });
                final Observable<AnalyticsMetrics> info = response.info().map(new Func1<ByteBuf, JsonObject>() {
                    @Override
                    public JsonObject call(ByteBuf byteBuf) {
                        try {
                            return JSON_OBJECT_TRANSCODER.byteBufToJsonObject(byteBuf);
                        } catch (Exception e) {
                            throw new TranscodingException("Could not decode N1QL Query Info.", e);
                        } finally {
                            byteBuf.release();
                        }
                    }
                })
                    .map(new Func1<JsonObject, AnalyticsMetrics>() {
                        @Override
                        public AnalyticsMetrics call(JsonObject jsonObject) {
                            return new AnalyticsMetrics(jsonObject);
                        }
                    });
                final Observable<String> finalStatus = response.queryStatus();
                final Observable<JsonObject> errors = response.errors().map(new Func1<ByteBuf, JsonObject>() {
                    @Override
                    public JsonObject call(ByteBuf byteBuf) {
                        try {
                            return JSON_OBJECT_TRANSCODER.byteBufToJsonObject(byteBuf);
                        } catch (Exception e) {
                            throw new TranscodingException("Could not decode View Info.", e);
                        } finally {
                            byteBuf.release();
                        }
                    }
                });
                boolean parseSuccess = response.status().isSuccess();
                String contextId = response.clientRequestId() == null ? "" : response.clientRequestId();
                String requestId = response.requestId();

                AsyncAnalyticsQueryResult r = new DefaultAsyncAnalyticsQueryResult(rows, signature, info, errors,
                    finalStatus, parseSuccess, requestId, contextId);
                return Observable.just(r);            }
        });
    }
}
