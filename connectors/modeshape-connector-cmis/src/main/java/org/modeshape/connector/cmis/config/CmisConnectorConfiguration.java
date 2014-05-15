package org.modeshape.connector.cmis.config;

import org.modeshape.connector.cmis.features.SingleVersionOptions;

/*
  * Container to keep parameters passed to connector
  * and pass then further
 */
public class CmisConnectorConfiguration {

    // to not reset required properties on document create
    private boolean ignoreEmptyPropertiesOnCreate = false;
    // add required properties to a document if not present
    private boolean addRequiredPropertiesOnRead = false;

    // sns index for optimization
    // -1 will calculate real value
    private int snsCommonIndex = 0; /*-1*/

    private String remoteUnfiledNodeId;

    private String unfiledQueryTemplate;

    private boolean folderSetUnknownChildren = false;

    long pageSize;

    long pageSizeUnfiled;
    
    int folderCacheTtlSeconds;
    int folderCacheSize;

    SingleVersionOptions singleVersionOptions;
    private boolean hideRootFolderReference;

    boolean debug;

    public CmisConnectorConfiguration(boolean ignoreEmptyPropertiesOnCreate,
                                      boolean addRequiredPropertiesOnRead, int snsCommonIndex,
                                      String remoteUnfiledNodeId, String unfiledQueryTemplate,
                                      boolean folderSetUnknownChildren, long pageSize, long pageSizeUnfiled,
                                      SingleVersionOptions singleVersionOptions,
                                      boolean hideRootFolderReference,
                                      boolean debug, int folderCacheTtlSeconds, int folderCacheSize) {
        this.ignoreEmptyPropertiesOnCreate = ignoreEmptyPropertiesOnCreate;
        this.addRequiredPropertiesOnRead = addRequiredPropertiesOnRead;
        this.snsCommonIndex = snsCommonIndex;
        this.remoteUnfiledNodeId = remoteUnfiledNodeId;
        this.unfiledQueryTemplate = unfiledQueryTemplate;
        this.folderSetUnknownChildren = folderSetUnknownChildren;
        this.pageSize = pageSize;
        this.pageSizeUnfiled = pageSizeUnfiled;
        this.singleVersionOptions = singleVersionOptions;
        this.hideRootFolderReference = hideRootFolderReference;
        this.debug = debug;
        this.folderCacheTtlSeconds = folderCacheTtlSeconds;
        this.folderCacheSize = folderCacheSize;
    }

    public boolean isIgnoreEmptyPropertiesOnCreate() {
        return ignoreEmptyPropertiesOnCreate;
    }

    public boolean isAddRequiredPropertiesOnRead() {
        return addRequiredPropertiesOnRead;
    }

    public int getSnsCommonIndex() {
        return snsCommonIndex;
    }

    public String getRemoteUnfiledNodeId() {
        return remoteUnfiledNodeId;
    }

    public String getUnfiledQueryTemplate() {
        return unfiledQueryTemplate;
    }

    public boolean isFolderSetUnknownChildren() {
        return folderSetUnknownChildren;
    }

    public long getPageSize() {
        return pageSize;
    }

    public long getPageSizeUnfiled() {
        return pageSizeUnfiled;
    }

    public SingleVersionOptions getSingleVersionOptions() {
        return singleVersionOptions;
    }

    public boolean isHideRootFolderReference() {
        return hideRootFolderReference;
    }

    public boolean isDebug() {
        return debug;
    }

    public int getFolderCacheTtlSeconds() {
        return folderCacheTtlSeconds;
    }

    public int getFolderCacheSize() {
        return folderCacheSize;
    }

}
