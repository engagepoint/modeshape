package org.modeshape.connector.cmis.operations.impl;

import org.apache.chemistry.opencmis.client.api.*;
import org.apache.chemistry.opencmis.client.runtime.OperationContextImpl;
import org.apache.chemistry.opencmis.commons.enums.BaseTypeId;
import org.modeshape.connector.cmis.CmisObjectFinderUtil;
import org.modeshape.connector.cmis.Constants;
import org.modeshape.connector.cmis.mapping.LocalTypeManager;
import org.modeshape.connector.cmis.ObjectId;
import org.modeshape.jcr.federation.spi.DocumentWriter;
import org.modeshape.jcr.federation.spi.PageKey;

import java.util.Iterator;

public class CmisGetChildrenOperation extends CmisOperation {

    private String remoteUnfiledNodeId;
    private String commonIdPropertyName;

    public CmisGetChildrenOperation(Session session, LocalTypeManager localTypeManager, String remoteUnfiledNodeId,
            String commonIdPropertyName,CmisObjectFinderUtil finderUtil) {
        super(session, localTypeManager, finderUtil);
        this.remoteUnfiledNodeId = remoteUnfiledNodeId;
        this.commonIdPropertyName= commonIdPropertyName;
    }

    /**
     * Converts CMIS folder children to JCR node children
     *
     * @param folder CMIS folder
     * @param writer JCR node representation
     */
    public void cmisChildren(Folder folder, DocumentWriter writer) {
        getChildren(new PageKey(folder.getId(), "0", Constants.DEFAULT_PAGE_SIZE), writer);
    }

    /*
    * get children for the page specified and add reference to new page if has mode children
    */
    public DocumentWriter getChildren(PageKey pageKey, DocumentWriter writer) {

        String parentId = pageKey.getParentId();
        ItemIterable<CmisObject> children;

        if (ObjectId.isUnfiledStorage(parentId)) {
            children = getUnfiledDocuments(pageKey);
        } else {
            Folder parent = (Folder) finderUtil.find(parentId);
            children = parent.getChildren();
        }

        int blockSize = (int) pageKey.getBlockSize();
        int offset = pageKey.getOffsetInt();
        ItemIterable<CmisObject> page = children.skipTo(offset);

        Iterator<CmisObject> pageIterator = page.iterator();
        for (int i = 0; pageIterator.hasNext() && i < blockSize; i++) {
            CmisObject next = page.iterator().next();
            String childId = (CmisOperationCommons.isDocument(next) && CmisOperationCommons.isVersioned(next))
                    ? CmisOperationCommons.asDocument(next).getVersionSeriesId()
                    : next.getId();

            // use common ID instead
            Property<Object> commonIdProp = next.getProperty(commonIdPropertyName);
            if (commonIdProp != null && commonIdProp.getValueAsString() != null) {
                writer.addChild(commonIdProp.getValueAsString(), next.getName());
            } else {
                writer.addChild(childId, next.getName());
            }
        }

        if (pageIterator.hasNext())
            writer.addPage(parentId, offset + 1, blockSize, children.getTotalNumItems());

        return writer;
    }


    /*
    * utilise CMIS query to get unfiled documents
    */
    private ItemIterable<CmisObject> getUnfiledDocuments(PageKey pageKey) {
        StringBuilder unfiledCondition = new StringBuilder();

        if (remoteUnfiledNodeId != null) {
            unfiledCondition.append("IN_FOLDER('").append(remoteUnfiledNodeId).append("')");
        } else {
            unfiledCondition.append("NOT (");

            ItemIterable<CmisObject> children = session.getRootFolder().getChildren();
            if (children.getTotalNumItems() > 0) {
                for (CmisObject child : children) {
                    if (child.getBaseTypeId().equals(BaseTypeId.CMIS_FOLDER)) {
                        unfiledCondition.append(" IN_TREE('").append(child.getId()).append("') AND ");
                    }
                }
                unfiledCondition.delete(unfiledCondition.lastIndexOf("AND"), unfiledCondition.length() - 1).append(")");
            }
        }

        debug("Unfiled where statement -- " + unfiledCondition.toString());

        OperationContextImpl context = new OperationContextImpl();
        ItemIterable<CmisObject> cmisObjects = session.queryObjects("cmis:document", unfiledCondition.toString(), false, context);
        debug("Got " + cmisObjects.getTotalNumItems() + " documents in the result");
        return cmisObjects.skipTo(pageKey.getOffsetInt());
    }
}
