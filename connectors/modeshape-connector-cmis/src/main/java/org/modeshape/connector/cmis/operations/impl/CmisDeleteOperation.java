package org.modeshape.connector.cmis.operations.impl;

import org.apache.chemistry.opencmis.client.api.CmisObject;
import org.apache.chemistry.opencmis.client.api.Folder;
import org.apache.chemistry.opencmis.client.api.Session;
import org.apache.chemistry.opencmis.commons.enums.UnfileObject;
import org.modeshape.connector.cmis.operations.CmisObjectFinderUtil;
import org.modeshape.connector.cmis.mapping.LocalTypeManager;
import org.modeshape.connector.cmis.ObjectId;


public class CmisDeleteOperation extends CmisOperation {

    public CmisDeleteOperation(Session session, LocalTypeManager localTypeManager,CmisObjectFinderUtil finderUtil) {
        super(session, localTypeManager,finderUtil);
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
        // these type points to either cmis:document or cmis:folder so
        // we can just delete it using original identifier defined in cmis domain.
        CmisObject object = null;
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

        return true;
    }

    /*
    * delete document content
    */
    private boolean doDeleteContent(ObjectId objectId) {
        // in the jcr domain content is represented by child node of
        // the nt:file node while in cmis domain it is a property of
        // the cmis:document object. so to perform this operation we need
        // to restore identifier of the original cmis:document. it is easy
        String cmisId = objectId.getIdentifier();

        org.apache.chemistry.opencmis.client.api.Document doc = null;
        try {
            doc = CmisOperationCommons.asDocument(finderUtil.find(cmisId));
        } catch (org.apache.chemistry.opencmis.commons.exceptions.CmisObjectNotFoundException nfe) {
            return true;
        }

        // object exists?
        if (doc == null) {
            // object does not exist. probably was deleted by from cmis domain
            // we don't know how to handle such case yet, thus TODO
            return false;
        }

        // delete content stream
        if (CmisOperationCommons.isVersioned(doc)) {
            // ignore deletion of content stream. lets rely on delete document then
            // may be called before document deletion. in this case may not be necessary
//                    deleteStreamVersioned(doc); ???
        } else {
            try {
                // let's getObject once again just before delete as it may not exist by the moment any more
                doc = CmisOperationCommons.asDocument(finderUtil.find(cmisId));
                if (doc != null) doc.deleteContentStream();
            } catch (org.apache.chemistry.opencmis.commons.exceptions.CmisObjectNotFoundException nfe) {
                return true;
            }

        }

        return true;
    }


    public void deleteStreamVersioned(CmisObject object) {
        org.apache.chemistry.opencmis.client.api.Document pwc = CmisOperationCommons.checkout(session, object);
        pwc.deleteContentStream();
    }

    public void deleteVersioned(CmisObject object) {
        org.apache.chemistry.opencmis.client.api.Document document = CmisOperationCommons.asDocument(object);
        if (document.isPrivateWorkingCopy() || document.isVersionSeriesCheckedOut())
            document.deleteAllVersions();

        org.apache.chemistry.opencmis.client.api.Document pwc = CmisOperationCommons.checkout(session, object);
        pwc.deleteAllVersions();
    }

}
