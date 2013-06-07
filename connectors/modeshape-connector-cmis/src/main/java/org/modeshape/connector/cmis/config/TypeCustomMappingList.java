package org.modeshape.connector.cmis.config;

import org.infinispan.schematic.document.Document;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class TypeCustomMappingList {

    Map<String, String> namespaces = new HashMap<String, String>();
    List<Document> types = new LinkedList<Document>();

    public Map<String, String> getNamespaces() {
        return namespaces;
    }

    public List<Document> getTypes() {
        return types;
    }
}
