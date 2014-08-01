package org.modeshape.connector.cmis.config;

import org.modeshape.connector.cmis.features.SingleVersionOptions;
import org.modeshape.jcr.mimetype.MimeTypeDetector;

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

    SingleVersionOptions singleVersionOptions;
    private boolean hideRootFolderReference;
    private String versioningOnUpdateMetadata;

    boolean debug;

    private String sourceName;

    private MimeTypeDetector mimeTypeDetector;

    public CmisConnectorConfiguration(boolean ignoreEmptyPropertiesOnCreate,
                                      boolean addRequiredPropertiesOnRead, int snsCommonIndex,
                                      String remoteUnfiledNodeId, String unfiledQueryTemplate,
                                      boolean folderSetUnknownChildren, long pageSize, long pageSizeUnfiled,
                                      SingleVersionOptions singleVersionOptions,
                                      boolean hideRootFolderReference,
                                      boolean debug, String versioningOnUpdateMetadata,
                                      String sourceName, MimeTypeDetector mimeTypeDetector) {
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
        this.versioningOnUpdateMetadata = versioningOnUpdateMetadata;
        this.sourceName = sourceName;
        this.mimeTypeDetector = mimeTypeDetector;
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

    public String getVersioningOnUpdateMetadata() {
        return versioningOnUpdateMetadata;
    }

    public String getSourceName() {
        return sourceName;
    }

    public MimeTypeDetector getMimeTypeDetector() {
        return mimeTypeDetector;
    }
}
