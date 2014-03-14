package org.modeshape.connector.cmis.operations.impl;

import org.apache.chemistry.opencmis.client.api.*;
import org.apache.chemistry.opencmis.client.runtime.OperationContextImpl;
import org.apache.chemistry.opencmis.commons.enums.BaseTypeId;
import org.modeshape.connector.cmis.operations.CmisObjectFinderUtil;
import org.modeshape.connector.cmis.Constants;
import org.modeshape.connector.cmis.features.SingleVersionOptions;
import org.modeshape.connector.cmis.mapping.LocalTypeManager;
import org.modeshape.connector.cmis.ObjectId;
import org.modeshape.jcr.federation.spi.DocumentWriter;
import org.modeshape.jcr.federation.spi.PageKey;
import org.modeshape.jcr.federation.spi.PageWriter;

import java.util.Iterator;
import java.util.NoSuchElementException;

public class CmisGetChildrenOperation extends CmisOperation {

    private String remoteUnfiledNodeId;
    private String commonIdPropertyName;
    private long pageSize;
    private boolean folderSetUnknownChildren;

    public CmisGetChildrenOperation(Session session, LocalTypeManager localTypeManager, String remoteUnfiledNodeId,
                                    SingleVersionOptions singleVersionOptions, CmisObjectFinderUtil finderUtil,
                                    long pageSize, boolean folderSetUnknownChildren) {
        super(session, localTypeManager, finderUtil);
        this.remoteUnfiledNodeId = remoteUnfiledNodeId;
        this.commonIdPropertyName = singleVersionOptions.getCommonIdPropertyName();
        this.pageSize = pageSize;
        this.folderSetUnknownChildren = folderSetUnknownChildren;
    }

    /**
     * Converts CMIS folder children to JCR node children
     *
     * @param folder CMIS folder
     * @param writer JCR node representation
     */
    public void cmisChildren(Folder folder, DocumentWriter writer) {
        String parentId = folder.getId();
        if (pageSize != Constants.NO_PAGING || ObjectId.isUnfiledStorage(parentId)) {
            getChildren(new PageKey(parentId, "0", pageSize), writer);
        } else {
            Folder parent = (Folder) finderUtil.find(parentId);
            ItemIterable<CmisObject> children = parent.getChildren();
            Iterator<CmisObject> iterator = children.iterator();
            // try to use next right away in order to save time for hasNext call
            CmisObject item = getNext(iterator);
            while (item != null) {
                String childId = finderUtil.getObjectMappingId(item);
                writer.addChild(childId, item.getName());
                item = getNext(iterator);
            }
        }
    }

    private CmisObject getNext(Iterator<CmisObject> iterator) {
        try {
            return iterator.next();
        } catch (NoSuchElementException nse) {
            return null;
        }
    }

    public DocumentWriter getChildren(PageKey pageKey, DocumentWriter writer) {
        return getChildren(pageKey, writer, (int) pageKey.getBlockSize());
    }


    /*
    * get children for the page specified and add reference to new page if has mode children
    */
    public DocumentWriter getChildren(PageKey pageKey, DocumentWriter writer, int nextBlockSize) {

        String parentId = pageKey.getParentId();
        ItemIterable<CmisObject> children;
        int blockSize = (int) pageKey.getBlockSize();
        int offset = pageKey.getOffsetInt();

        boolean unfiledStorage = ObjectId.isUnfiledStorage(parentId);
        if (unfiledStorage) {
            children = getUnfiledDocuments(pageKey.getOffsetInt(), blockSize);
        } else {
            Folder parent = (Folder) finderUtil.find(parentId);
            OperationContext ctx = session.createOperationContext();
            ctx.setMaxItemsPerPage(blockSize);
//            ctx.setOrderBy("cmis:creationDate DESC");
//            ctx.setCacheEnabled(true);
            children = parent.getChildren(ctx);
        }

        ItemIterable<CmisObject> page = children.skipTo(offset);

        Iterator<CmisObject> pageIterator = page.iterator();
        debug("adding children for pageKey = " + pageKey);
        for (int i = 0; pageIterator.hasNext() && i < blockSize; i++) {
            CmisObject next = page.iterator().next();
            String childId = finderUtil.getObjectMappingId(next);
            debug("adding child", next.getName(), childId);
            writer.addChild(childId, next.getName());
        }


        if (pageIterator.hasNext()) {
            int nextPageOffset = offset + blockSize;
            long totalSize = (!unfiledStorage && folderSetUnknownChildren)
                    ? PageWriter.UNKNOWN_TOTAL_SIZE
                    : children.getTotalNumItems();
            debug("adding follower page " + nextPageOffset + "#" + nextBlockSize + " " + totalSize);
            writer.addPage(parentId, nextPageOffset, nextBlockSize, totalSize);
        }

        return writer;
    }


    /*
    * utilise CMIS query to get unfiled documents
    */
    private ItemIterable<CmisObject> getUnfiledDocuments(int skipTo, int pageSize) {
        StringBuilder unfiledCondition = new StringBuilder();

        if (remoteUnfiledNodeId != null) {
            unfiledCondition.append("IN_FOLDER('").append(remoteUnfiledNodeId).append("')");
        } else {
            unfiledCondition.append("NOT (");

            ItemIterable<CmisObject> children = session.getRootFolder().getChildren();
            if (children != null) {
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
        context.setMaxItemsPerPage(pageSize);
        ItemIterable<CmisObject> cmisObjects = session.queryObjects("cmis:document", unfiledCondition.toString(), false, context);
        debug("Got " + cmisObjects.getTotalNumItems() + " documents in the result");
        return cmisObjects.skipTo(skipTo);
    }
}
