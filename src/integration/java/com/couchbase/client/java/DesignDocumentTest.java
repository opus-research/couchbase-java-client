package com.couchbase.client.java;

import com.couchbase.client.java.util.ClusterDependentTest;
import com.couchbase.client.java.view.DesignDocument;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;

public class DesignDocumentTest extends ClusterDependentTest {

    @Test
    public void shouldGetDesignDocument() {
        List<DesignDocument> designDocuments = bucket().getDesignDocument("beer").toList().toBlocking().single();
        assertEquals(2, designDocuments.size());
    }

    @Test
    public void shouldListDesignDocuments() {
        List<DesignDocument> designDocuments = bucket().listDesignDocuments().toList().toBlocking().single();
        System.err.println(designDocuments.size());
        System.out.println(designDocuments.get(0).views().get(1));
    }

}
