package org.modeshape.connector.cmis.operations;

import org.apache.chemistry.opencmis.commons.data.ContentStream;
import org.infinispan.schematic.document.Document;

public interface BinaryContentProducerInterface {

    ContentStream jcrBinaryContent(Document document, String fileName);

}
