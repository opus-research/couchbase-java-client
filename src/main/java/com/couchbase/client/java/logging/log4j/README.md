# Couchbase Log4j Appenders
This package provides Log appenders for the Log4j project. The main goal is to log events to a Couchbase Server
bucket, instead (or in addition to) regular log files or other sources.

## Configuration
In addition to the Couchbase Java SDK, Log4j needs to be added as a dependency to your project.

```xml
<dependencies>
    <dependency>
        <groupId>com.couchbase.client</groupId>
        <artifactId>java-client</artifactId>
        <version>2.0.2</version>
    </dependency>
    <dependency>
        <groupId>log4j</groupId>
        <artifactId>log4j</artifactId>
        <version>1.2.17</version>
    </dependency>
</dependencies>
```

Now the appender can be configured like any other, here is a `log4j.properties` with all possible options:

```
log4j.rootLogger=INFO, cb

log4j.appender.cb=com.couchbase.client.java.logging.log4j.CouchbaseAppender
log4j.appender.cb.applicationId=myApp
log4j.appender.cb.hosts=127.0.0.1
log4j.appender.cb.bucket=logs
log4j.appender.cb.password=
```

If you want to stick with the defaults as shown (connect to `127.0.0.1` and use the `logs` bucket with no password), then
you can also use this one-liner:

```
log4j.appender.cb=com.couchbase.client.java.logging.log4j.CouchbaseAppender
```

In addition to the synchronous appender, the same options are available for the `CouchbaseAsyncAppender`, which does
not wait until the log insert completes, but just keeps moving on (errors are still logged through a callback to make
it diagnosable).

```
log4j.appender.cb=com.couchbase.client.java.logging.log4j.CouchbaseAsyncAppender
```

Note that the async appender has the drawback that log entries might be lost in error situations, as well as you can't
be sure that everything is logged during a shutdown, since no one waits until they are completely written. So only use
it when you don't care about lost messages and want highest throughput without holding up the application.

You can then start logging:

```java
import org.apache.log4j.Logger;

public class Main {

    private static Logger logger = Logger.getLogger(Main.class);

    public static void main(String[] args) throws Exception {
        logger.info("Hello World!");
    }

}
```

This will store a document looking like this in the bucket:

```json
{
  "level": "INFO",
  "name": "Main",
  "applicationId": "testApp",
  "message": "Hello World!",
  "properties": {
    "applicationId": "myApp"
  },
  "timestamp": 1416917036253
}
```

If exceptions are logged, you get more context:

```json
  "level": "INFO",
  "name": "Main",
  "applicationId": "testApp",
  "message": "Hello World!",
  "traceback": [
    "java.lang.Exception: This is an exception",
    "\tat Main.main(Main.java:8)"
  ],
  "properties": {
    "applicationId": "testApp"
  },
  "timestamp": 1416917227684
}
```

