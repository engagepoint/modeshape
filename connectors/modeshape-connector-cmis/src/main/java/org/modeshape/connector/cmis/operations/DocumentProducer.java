package org.modeshape.connector.cmis.operations;


import org.modeshape.jcr.federation.spi.DocumentWriter;

public interface DocumentProducer {

    DocumentWriter getNewDocument(String id);
}
