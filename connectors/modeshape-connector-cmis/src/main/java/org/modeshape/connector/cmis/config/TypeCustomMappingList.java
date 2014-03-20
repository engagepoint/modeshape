package org.modeshape.connector.cmis.config;

import org.infinispan.schematic.document.Document;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class TypeCustomMappingList {

    Map<String, String> namespaces = new HashMap<String, String>();
    List<Document> types = new LinkedList<Document>();
    Set<String> globalIgnoredExtProperties = new HashSet<String>(10);

    public Map<String, String> getNamespaces() {
        return namespaces;
    }

    public List<Document> getTypes() {
        return types;
    }

    public Set<String> getGlobalIgnoredExtProperties() {
        return globalIgnoredExtProperties;
    }
}
