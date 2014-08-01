package org.modeshape.connector.cmis.operations.impl;

import org.apache.chemistry.opencmis.client.api.CmisObject;
import org.apache.chemistry.opencmis.client.api.Folder;
import org.apache.chemistry.opencmis.commons.enums.UnfileObject;
import org.modeshape.connector.cmis.RuntimeSnapshot;
import org.modeshape.connector.cmis.config.CmisConnectorConfiguration;
import org.modeshape.connector.cmis.ObjectId;


public class CmisDeleteOperation extends CmisOperation {

    public CmisDeleteOperation(RuntimeSnapshot snapshot,
                               CmisConnectorConfiguration config) {
        super(snapshot, config);
    }

    public boolean removeDocument(String id) {
        ObjectId objectId = ObjectId.valueOf(id);

        switch (objectId.getType()) {
            case REPOSITORY_INFO:
                // information about repository is ready only
                return false;
            case CONTENT:
                return doDeleteContent(objectId);

            case OBJECT:
                return doDeleteObject(objectId);

            default:
                return false;
        }
    }

    /*
    * delete document/folder
    */
    private boolean doDeleteObject(ObjectId objectId) {
        long startTime = System.currentTimeMillis();
        String id = objectId == null ? "null" : objectId.getIdentifier();
        debug("Start CmisDeleteOperation:doDeleteObject for objectId = ", id);
        // these type points to either cmis:document or cmis:folder so
        // we can just delete it using original identifier defined in cmis domain.
        CmisObject object;
        try {
            object = finderUtil.find(objectId.getIdentifier());
            if (object == null) return true;

            if (object instanceof Folder) {
                // don't care about unfiling vs delete for now
                ((Folder) object).deleteTree(true, UnfileObject.DELETE, false);
            } else {
                // delete document
                object.delete(true /*all versions*/);
            }
        } catch (org.apache.chemistry.opencmis.commons.exceptions.CmisObjectNotFoundException nfe) {
            return true;
        }
        if (snapshot.getCache() != null) {
            snapshot.getCache().remove(id);
        }
        debug("Finish CmisDeleteOperation:doDeleteObject for objectId = ", id, ". Time:", Long.toString(System.currentTimeMillis()-startTime), "ms");
        return true;
    }

    /*
    * delete document content
    */
    private boolean doDeleteContent(ObjectId objectId) {
        long startTime = System.currentTimeMillis();
        // in the jcr domain content is represented by child node of
        // the nt:file node while in cmis domain it is a property of
        // the cmis:document object. so to perform this operation we need
        // to restore identifier of the original cmis:document. it is easy
        String cmisId = objectId.getIdentifier();
        debug("Start CmisDeleteOperation:doDeleteContent for objectId = ", cmisId);
        
        CmisObject cmisObject = null;
        try {
            cmisObject = finderUtil.find(cmisId);
        } catch (org.apache.chemistry.opencmis.commons.exceptions.CmisObjectNotFoundException nfe) {
            return true;
        }

        // object exists?
        if (cmisObject == null) {
            // object does not exist. probably was deleted by from cmis domain
            // we don't know how to handle such case yet, thus TODO
            return false;
        }

        // delete content stream
        if (CmisOperationCommons.isVersioned(CmisOperationCommons.asDocument(cmisObject))) {
            // ignore deletion of content stream. lets rely on delete document then
            // may be called before document deletion. in this case may not be necessary
//                    deleteStreamVersioned(doc); ???
            // checkout -> deleteContentStream -> ??

        } else {
            try {
                // let's getObject once again just before delete as it may not exist by the moment any more
                cmisObject = finderUtil.find(cmisId);
                if (cmisObject != null) CmisOperationCommons.asDocument(cmisObject).deleteContentStream();
            } catch (org.apache.chemistry.opencmis.commons.exceptions.CmisObjectNotFoundException nfe) {
                return true;
            }

        }
        if (snapshot.getCache() != null) {
            snapshot.getCache().remove(cmisId);
        }
        debug("Finish CmisDeleteOperation:doDeleteContent for objectId = ", cmisId, ". Time:", Long.toString(System.currentTimeMillis()-startTime), "ms");
        return true;
    }


    public void deleteStreamVersioned(CmisObject object) {
        long startTime = System.currentTimeMillis();
        debug("Start CmisDeleteOperation:deleteStreamVersioned for objectId = ", object == null ? "null" : object.getId());
        org.apache.chemistry.opencmis.client.api.Document pwc = CmisOperationCommons.checkout(session, object);
        pwc.deleteContentStream();
        if (snapshot.getCache() != null) {
            snapshot.getCache().remove(object.getId());
        }
        debug("Finish CmisDeleteOperation:deleteStreamVersioned for objectId = ", object == null ? "null" : object.getId(), " Time:", Long.toString(System.currentTimeMillis()-startTime), "ms");
    }

    public void deleteVersioned(CmisObject object) {
        long startTime = System.currentTimeMillis();
        debug("Start CmisDeleteOperation:deleteVersioned for objectId = ", object == null ? "null" : object.getId());
        org.apache.chemistry.opencmis.client.api.Document document = CmisOperationCommons.asDocument(object);
        if (document.isPrivateWorkingCopy() || document.isVersionSeriesCheckedOut())
            document.deleteAllVersions();

        org.apache.chemistry.opencmis.client.api.Document pwc = CmisOperationCommons.checkout(session, object);
        pwc.deleteAllVersions();
        if (snapshot.getCache() != null) {
            snapshot.getCache().remove(document.getId());
        }
        debug("Finish CmisDeleteOperation:deleteVersioned for objectId = ", object == null ? "null" : object.getId(), ". Time:", Long.toString(System.currentTimeMillis()-startTime), "ms");
    }

}
