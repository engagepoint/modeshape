package org.modeshape.connector.cmis.operations.impl;

import org.apache.chemistry.opencmis.client.api.*;
import org.apache.chemistry.opencmis.client.runtime.OperationContextImpl;
import org.apache.chemistry.opencmis.commons.PropertyIds;
import org.apache.commons.lang3.StringUtils;
import org.modeshape.connector.cmis.Constants;
import org.modeshape.connector.cmis.ObjectId;
import org.modeshape.connector.cmis.RuntimeSnapshot;
import org.modeshape.connector.cmis.config.CmisConnectorConfiguration;
import org.modeshape.jcr.federation.spi.DocumentWriter;
import org.modeshape.jcr.federation.spi.PageKey;
import org.modeshape.jcr.federation.spi.PageWriter;

import java.util.Iterator;
import java.util.NoSuchElementException;

public class CmisGetChildrenOperation extends CmisOperation {

    public CmisGetChildrenOperation(RuntimeSnapshot snapshot,
                                    CmisConnectorConfiguration config) {
        super(snapshot, config);
    }

    /**
     * Converts CMIS folder children to JCR node children
     *
     * @param folder CMIS folder
     * @param writer JCR node representation
     */
    public void cmisChildren(Folder folder, DocumentWriter writer) {
        long startTime = System.currentTimeMillis();
        debug("Start CmisGetChildrenOperation:cmisChildren for folder = ", folder == null ? "null" : folder.getName());
        String parentId = folder.getId();
        if (config.getPageSize() != Constants.NO_PAGING || ObjectId.isUnfiledStorage(parentId)) {
            if (ObjectId.isUnfiledStorage(parentId)) {
                getChildren(new PageKey(parentId, "0", config.getPageSizeUnfiled()), writer, 0);
            } else {
                getChildren(new PageKey(parentId, "0", config.getPageSize()), writer);
            }
        } else {
            Folder parent = (Folder) finderUtil.find(parentId);
            OperationContext ctx = session.createOperationContext();
            ctx.setMaxItemsPerPage(1000); // check if it affects performance
            ctx.setCacheEnabled(true); 
            ctx.setIncludeAllowableActions(false);
            ItemIterable<CmisObject> children = parent.getChildren(ctx);
            Iterator<CmisObject> iterator = children.iterator();
            // try to use next right away in order to save time for hasNext call
            CmisObject item = getNext(iterator);
            while (item != null) {
                String childId = finderUtil.getObjectMappingId(item);
                writer.addChild(childId, item.getName());
                item = getNext(iterator);
            }
        }
        debug("Finish CmisGetChildrenOperation:cmisChildren for folder = ", folder.getName(), ". Time:", Long.toString(System.currentTimeMillis()-startTime), "ms");
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
    public DocumentWriter getChildren(PageKey pageKey, DocumentWriter writer, int currentBlockSize) {
        long startTime = System.currentTimeMillis();
        debug("Start CmisGetChildrenOperation:getChildren adding children for ", pageKey == null ? "null" : pageKey.toString(), " with currentPageSize ", Integer.toString(currentBlockSize));
        String parentId = pageKey.getParentId();

        ItemIterable<?> children;
        int blockSize = currentBlockSize;
        int offset = pageKey.getOffsetInt();

        boolean unfiledStorage = ObjectId.isUnfiledStorage(parentId);
        if (unfiledStorage) {
            boolean doAddPage = blockSize > 0;

            if (blockSize > 0) {
                children = getUnfiledDocuments(pageKey.getOffsetInt(), blockSize);
                ItemIterable<?> page = children;
                Iterator<?> pageIterator = page.iterator();
                for (int i = 0; pageIterator.hasNext() && i < blockSize; i++) {
                    QueryResult next = (QueryResult) page.iterator().next();
                    String childId = finderUtil.getObjectMappingId(next);
                    String oName = next.getPropertyById(PropertyIds.NAME).getFirstValue().toString();
                    writer.addChild(childId, oName);
                }
                doAddPage = pageIterator.hasNext();
            }

            if (doAddPage) {
                int nextPageOffset = offset + blockSize;
                long totalSize = PageWriter.UNKNOWN_TOTAL_SIZE;
                debug("adding follower page offset/size: " + nextPageOffset + "#" + (int) pageKey.getBlockSize() + " " + totalSize);
                writer.addPage(parentId, nextPageOffset, pageKey.getBlockSize(), totalSize);
            }
        } else {
            Folder parent = (Folder) finderUtil.find(parentId);
            OperationContext ctx = session.createOperationContext();
            ctx.setMaxItemsPerPage(1000); // check if it affects performance
            ctx.setIncludeAllowableActions(false);            
            ctx.setCacheEnabled(true); 
            children = parent.getChildren(ctx);

            ItemIterable<?> page = children.skipTo(offset);
            Iterator<?> pageIterator = page.iterator();
            for (int i = 0; pageIterator.hasNext() && i < blockSize; i++) {
                CmisObject next = (CmisObject) pageIterator.next();
                String childId = finderUtil.getObjectMappingId(next);
                writer.addChild(childId, next.getName());
            }

            if (pageIterator.hasNext()) {
                int nextPageOffset = offset + blockSize;
                long totalSize = (config.isFolderSetUnknownChildren())
                        ? PageWriter.UNKNOWN_TOTAL_SIZE
                        : children.getTotalNumItems();
                debug("adding follower page offset/size: " + nextPageOffset + "#" + (int) pageKey.getBlockSize() + " / total = " + totalSize);
                writer.addPage(parentId, nextPageOffset, pageKey.getBlockSize(), totalSize);
            }
        }
        debug("Finish CmisGetChildrenOperation:getChildren for ", pageKey.toString(), ". Time:", Long.toString(System.currentTimeMillis()-startTime), "ms");
        return writer;
    }

    private String getUnfiledQueryTemplate() {
        return (StringUtils.isNotEmpty(config.getRemoteUnfiledNodeId()))
                ? "select * from cmis:document where " +
                "IN_FOLDER('" + config.getRemoteUnfiledNodeId() + "')"
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
