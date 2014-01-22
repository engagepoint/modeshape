package org.modeshape.connector.cmis.features;

import org.infinispan.schematic.document.Document;
import org.modeshape.jcr.value.Name;

/**
 * Created with IntelliJ IDEA.
 * User: dr_nickson
 * Date: 1/18/14
 * Time: 9:27 PM
 * To change this template use File | Settings | File Templates.
 */
public class TempDocument {

    String parentId;
    Name name;
    Name primaryType;
    Document document;

    public TempDocument(String parentId, Name name, Name primaryType) {
        this.parentId = parentId;
        this.name = name;
        this.primaryType = primaryType;
    }

    public Document getDocument() {
        return document;
    }

    public void setDocument(Document document) {
        this.document = document;
    }

    public String getParentId() {
        return parentId;
    }

    public Name getName() {
        return name;
    }

    public Name getPrimaryType() {
        return primaryType;
    }
}
