package org.modeshape.connector.cmis.operations.impl;

import org.apache.chemistry.opencmis.client.api.*;
import org.apache.chemistry.opencmis.client.runtime.OperationContextImpl;
import org.apache.chemistry.opencmis.commons.PropertyIds;
import org.apache.chemistry.opencmis.commons.enums.BaseTypeId;
import org.apache.commons.lang3.StringUtils;
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
    private String unfiledQueryTemplate;

    public CmisGetChildrenOperation(Session session, LocalTypeManager localTypeManager, String remoteUnfiledNodeId,
                                    SingleVersionOptions singleVersionOptions, CmisObjectFinderUtil finderUtil,
                                    long pageSize, boolean folderSetUnknownChildren,
                                    String unfiledQueryTemplate) {
        super(session, localTypeManager, finderUtil);
        this.remoteUnfiledNodeId = remoteUnfiledNodeId;
        this.commonIdPropertyName = singleVersionOptions.getCommonIdPropertyName();
        this.pageSize = pageSize;
        this.folderSetUnknownChildren = folderSetUnknownChildren;
        this.unfiledQueryTemplate = unfiledQueryTemplate;
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

//        if (parentId.equals()) {
//             writer.addChild(ObjectId.toString(ObjectId.Type.UNFILED_STORAGE, ""), ObjectId.Type.UNFILED_STORAGE.getValue());
//            return writer;
//        }

        ItemIterable<?> children;
        int blockSize = (int) pageKey.getBlockSize();
        int offset = pageKey.getOffsetInt();

        boolean unfiledStorage = ObjectId.isUnfiledStorage(parentId);
        if (unfiledStorage) {
            children = getUnfiledDocuments(pageKey.getOffsetInt(), blockSize);

            ItemIterable<?> page = children.skipTo(offset);
            Iterator<?> pageIterator = page.iterator();
            debug("adding children for pageKey = " + pageKey);
            for (int i = 0; pageIterator.hasNext() && i < blockSize; i++) {
                QueryResult next = (QueryResult) page.iterator().next();
                String childId = finderUtil.getObjectMappingId(next);
                String oName = next.getPropertyById(PropertyIds.NAME).getFirstValue().toString();
                debug("adding child", oName, childId);
                writer.addChild(childId, oName);
            }

            if (pageIterator.hasNext()) {
                int nextPageOffset = offset + blockSize;
                long totalSize = (!unfiledStorage && folderSetUnknownChildren)
                        ? PageWriter.UNKNOWN_TOTAL_SIZE
                        : children.getTotalNumItems();
                debug("adding follower page " + nextPageOffset + "#" + nextBlockSize + " " + totalSize);
                writer.addPage(parentId, nextPageOffset, nextBlockSize, totalSize);
            }
        } else {
            Folder parent = (Folder) finderUtil.find(parentId);
            OperationContext ctx = session.createOperationContext();
            // ? why this doesn't work for page size ??
            // return totalNumItem = pageSize instead of real total num items
            ctx.setMaxItemsPerPage(Integer.MAX_VALUE); // check if it affects performance
//            ctx.setMaxItemsPerPage(blockSize);
//            ctx.setOrderBy("cmis:creationDate DESC");
//            ctx.setCacheEnabled(true);
            children = parent.getChildren(ctx);

            ItemIterable<?> page = children.skipTo(offset);
            Iterator<?> pageIterator = page.iterator();
            debug("adding children for pageKey = " + pageKey);
            for (int i = 0; pageIterator.hasNext() && i < blockSize; i++) {
                CmisObject next = (CmisObject) page.iterator().next();
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
        }

        return writer;
    }

    private String getUnfiledQueryTemplate() {
        return (StringUtils.isNotEmpty(remoteUnfiledNodeId))
                    ? "select * from cmis:document where " +
                    "IN_FOLDER('" + remoteUnfiledNodeId + "')"
                    : "select doc.* from cmis:document doc " +
                    "LEFT JOIN ReferentialContainmentRelationship rcr ON document.This=rcr.Head " +
                    "WHERE rcr.Head is NULL";
    }


    /*
    * utilise CMIS query to get unfiled documents
    */
    private ItemIterable<?> getUnfiledDocuments(int skipTo, int pageSize) {
        String unfiledCondition = getUnfiledQueryTemplate();
        debug("Unfiled where statement -- " + unfiledCondition.toString());

        OperationContextImpl context = new OperationContextImpl();
        context.setMaxItemsPerPage(pageSize);
        ItemIterable<QueryResult> query = session.query(unfiledCondition, false, context);
//        ItemIterable<CmisObject> cmisObjects = session.queryObjects("cmis:document", unfiledCondition.toString(), false, context);
        debug("Got " + query.getTotalNumItems() + " documents in the result");
        return query.skipTo(skipTo);
    }
}
