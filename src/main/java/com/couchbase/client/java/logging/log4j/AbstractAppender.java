package com.couchbase.client.java.logging.log4j;

import com.couchbase.client.core.logging.CouchbaseLoggerFactory;
import com.couchbase.client.core.logging.Log4JLoggerFactory;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.json.JsonArray;
import com.couchbase.client.java.document.json.JsonObject;
import org.apache.log4j.AppenderSkeleton;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.MDC;
import org.apache.log4j.helpers.LogLog;
import org.apache.log4j.spi.LoggingEvent;

import java.util.Arrays;
import java.util.Calendar;
import java.util.Map;
import java.util.UUID;


public abstract class AbstractAppender extends AppenderSkeleton {


    public static final String LEVEL = "level";
    public static final String NAME = "name";
    public static final String APP_ID = "applicationId";
    public static final String TIMESTAMP = "timestamp";
    public static final String PROPERTIES = "properties";
    public static final String TRACEBACK = "traceback";
    public static final String MESSAGE = "message";
    public static final String YEAR = "year";
    public static final String MONTH = "month";
    public static final String DAY = "day";
    public static final String HOUR = "hour";

    private String bucket = "logs";
    private String password = "";
    private String hosts = "127.0.0.1";
    private int expiry = 0;

    private volatile Bucket bucketRef = null;
    private volatile boolean failedBootstrap = false;

    protected String applicationId = System.getProperty("APPLICATION_ID", null);

    static {
        CouchbaseLoggerFactory.setDefaultFactory(new Log4JLoggerFactory());
    }

    protected AbstractAppender() {
    }

    protected AbstractAppender(boolean isActive) {
        super(isActive);
    }

    public String getHosts() {
        return hosts;
    }

    public void setHosts(String hosts) {
        this.hosts = hosts;
    }

    public String getBucket() {
        return bucket;
    }

    public void setBucket(String bucket) {
        this.bucket = bucket;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getApplicationId() {
        return applicationId;
    }

    public void setApplicationId(String applicationId) {
        this.applicationId = applicationId;
    }

    public Bucket getBucketRef() {
        return bucketRef;
    }

    public void setBucketRef(Bucket bucketRef) {
        this.bucketRef = bucketRef;
    }

    public int getExpiry() {
        return expiry;
    }

    public void setExpiry(int expiry) {
        this.expiry = expiry;
    }

    private synchronized void connect() {
        if (bucketRef != null ||  failedBootstrap) {
            return;
        }

        Level global = LogManager.getLoggerRepository().getThreshold();

        try {
            LogManager.getLoggerRepository().setThreshold(Level.OFF);
            Cluster cluster = hosts.contains(",")
                ? CouchbaseCluster.create(hosts.split(",")) : CouchbaseCluster.create(hosts);
            bucketRef = cluster.openBucket(bucket, password);
        } catch(Throwable err) {
            failedBootstrap = true;
            LogLog.error("Could not initialize Couchbase Logger", err);
            errorHandler.error("Could not initialize Couchbase Logger" + err);
        } finally {
            LogManager.getLoggerRepository().setThreshold(global);
        }
    }

    protected abstract void doAppend(JsonDocument document);

    @Override
    @SuppressWarnings({ "unchecked" })
    protected void append(final LoggingEvent event) {
        if (failedBootstrap) {
            return;
        }

        if (bucketRef == null) {
            connect();
            if (failedBootstrap || bucketRef == null) {
                return;
            }
        }

        JsonObject target = JsonObject.create();

        if (null != applicationId) {
            target.put(APP_ID, applicationId);
            MDC.put(APP_ID, applicationId);
        }

        // Set logger name, level and timestamp
        target.put(NAME, event.getLogger().getName());
        target.put(LEVEL, event.getLevel().toString());
        target.put(TIMESTAMP, event.getTimeStamp());

        // Set properties
        Map<Object, Object> props = event.getProperties();
        if (props.size() > 0) {
            JsonObject propsObj = JsonObject.create();
            for (Map.Entry<Object, Object> entry : props.entrySet()) {
                propsObj.put(entry.getKey().toString(), entry.getValue().toString());
            }
            target.put(PROPERTIES, propsObj);
        }

        // Set traceback info if any
        String[] traceback = event.getThrowableStrRep();
        if (null != traceback && traceback.length > 0) {
            target.put(TRACEBACK, JsonArray.from(Arrays.asList(traceback)));
        }

        // Set the message
        target.put(MESSAGE, event.getRenderedMessage());

        // Update the log context
        Calendar now = Calendar.getInstance();
        MDC.put(YEAR, now.get(Calendar.YEAR));
        MDC.put(MONTH, String.format("%1$02d", now.get(Calendar.MONTH) + 1));
        MDC.put(DAY, String.format("%1$02d", now.get(Calendar.DAY_OF_MONTH)));
        MDC.put(HOUR, String.format("%1$02d", now.get(Calendar.HOUR_OF_DAY)));

        MDC.remove(YEAR);
        MDC.remove(MONTH);
        MDC.remove(DAY);
        MDC.remove(HOUR);
        if (null != applicationId) {
            MDC.remove(APP_ID);
        }

        doAppend(JsonDocument.create(generateId(), expiry, target));

    }

    @Override
    public void close() {
        if (bucketRef != null) {
            bucketRef.close();
        }
    }

    @Override
    public boolean requiresLayout() {
        return false;
    }

    /**
     * Generates a unique ID used for the document id.
     *
     * @return the unique document ID.
     */
    private static String generateId() {
        return UUID.randomUUID().toString();
    }

}
