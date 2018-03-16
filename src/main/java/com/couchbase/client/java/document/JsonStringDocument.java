package com.couchbase.client.java.document;

/**
 * .
 *
 * @author Michael Nitschinger
 */
public class JsonStringDocument extends AbstractDocument<String> {

    /**
     * Creates a empty {@link JsonStringDocument}.
     *
     * @return a empty {@link JsonStringDocument}.
     */
    public static JsonStringDocument empty() {
        return new JsonStringDocument(null, 0, null, 0);
    }

    /**
     * Creates a {@link JsonStringDocument} which the document id.
     *
     * @param id the per-bucket unique document id.
     * @return a {@link JsonStringDocument}.
     */
    public static JsonStringDocument create(String id) {
        return new JsonStringDocument(id, 0, null, 0);
    }

    /**
     * Creates a {@link JsonStringDocument} which the document id and content.
     *
     * @param id the per-bucket unique document id.
     * @param content the content of the document.
     * @return a {@link JsonStringDocument}.
     */
    public static JsonStringDocument create(String id, String content) {
        return new JsonStringDocument(id, 0, content, 0);
    }

    /**
     * Creates a {@link JsonStringDocument} which the document id, content and the CAS value.
     *
     * @param id the per-bucket unique document id.
     * @param content the content of the document.
     * @param cas the CAS (compare and swap) value for optimistic concurrency.
     * @return a {@link JsonStringDocument}.
     */
    public static JsonStringDocument create(String id, String content, long cas) {
        return new JsonStringDocument(id, 0, content, cas);
    }

    /**
     * Creates a {@link JsonStringDocument} which the document id, content and the expiration time.
     *
     * @param id the per-bucket unique document id.
     * @param content the content of the document.
     * @param expiry the expiration time of the document.
     * @return a {@link JsonStringDocument}.
     */
    public static JsonStringDocument create(String id, int expiry, String content) {
        return new JsonStringDocument(id, expiry, content, 0);
    }

    /**
     * Creates a {@link JsonStringDocument} which the document id, content, CAS value, expiration time and status code.
     *
     * This factory method is normally only called within the client library when a response is analyzed and a document
     * is returned which is enriched with the status code. It does not make sense to pre populate the status field from
     * the user level code.
     *
     * @param id the per-bucket unique document id.
     * @param content the content of the document.
     * @param cas the CAS (compare and swap) value for optimistic concurrency.
     * @param expiry the expiration time of the document.
     * @return a {@link JsonStringDocument}.
     */
    public static JsonStringDocument create(String id, int expiry, String content, long cas) {
        return new JsonStringDocument(id, expiry, content, cas);
    }

    /**
     * Creates a copy from a different {@link JsonStringDocument}, but changes the document ID and content.
     *
     * @param doc the original {@link JsonStringDocument} to copy.
     * @param id the per-bucket unique document id.
     * @param content the content of the document.
     * @return a copied {@link JsonStringDocument} with the changed properties.
     */
    public static JsonStringDocument from(JsonStringDocument doc, String id, String content) {
        return JsonStringDocument.create(id, doc.expiry(), content, doc.cas());
    }

    /**
     * Creates a copy from a different {@link JsonStringDocument}, but changes the CAS value.
     *
     * @param doc the original {@link JsonStringDocument} to copy.
     * @param cas the CAS (compare and swap) value for optimistic concurrency.
     * @return a copied {@link JsonStringDocument} with the changed properties.
     */
    public static JsonStringDocument from(JsonStringDocument doc, long cas) {
        return JsonStringDocument.create(doc.id(), doc.expiry(), doc.content(), cas);
    }

    /**
     * Private constructor which is called by the static factory methods eventually.
     *
     * @param id the per-bucket unique document id.
     * @param content the content of the document.
     * @param cas the CAS (compare and swap) value for optimistic concurrency.
     * @param expiry the expiration time of the document.
     */
    private JsonStringDocument(String id, int expiry, String content, long cas) {
        super(id, expiry, content, cas);
    }
}
