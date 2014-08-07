package org.modeshape.connector.cmis.features;

import org.apache.chemistry.opencmis.commons.data.ContentStream;
import org.modeshape.connector.cmis.operations.CmisObjectFinderUtil;
import org.modeshape.jcr.mimetype.MimeTypeDetector;
import org.modeshape.jcr.value.BinaryKey;
import org.modeshape.jcr.value.binary.ExternalBinaryValue;

import java.io.BufferedInputStream;
import java.io.InputStream;

/**
 * Created by vyacheslav.polulyakh on 7/25/2014.
 */
public class CmisBinaryValue extends ExternalBinaryValue {

    private static final long serialVersionUID = 1L;
    private transient CmisObjectFinderUtil finderUtil;
    private String id;

    public CmisBinaryValue(String id,
                           CmisObjectFinderUtil finderUtil,
                           String sourceName,
                           MimeTypeDetector mimeTypeDetector) {
        super(new BinaryKey(id), sourceName, id, 0, null, mimeTypeDetector);
        this.finderUtil = finderUtil;
        this.id = id;
    }

    @Override
    protected InputStream internalStream() throws Exception {
        ContentStream contentStream = finderUtil.getContentStream(id);
        return contentStream != null ? new BufferedInputStream(contentStream.getStream()) : null;
    }
}

