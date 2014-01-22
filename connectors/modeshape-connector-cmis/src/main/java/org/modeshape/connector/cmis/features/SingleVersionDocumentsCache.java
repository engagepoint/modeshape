package org.modeshape.connector.cmis.features;

import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class SingleVersionDocumentsCache {

    private Map<String, TempDocument> keyCache = new HashMap<String, TempDocument>();
    private Map<String, List<String>> keyChildrenCache = new HashMap<String, List<String>>();


    public TempDocument get(String key) {
        return keyCache.get(key);
    }

    public List<String> getReferences(String key) {
        return keyChildrenCache.get(key);
    }

    public void put(String key, TempDocument document) {
        keyCache.put(key, document);
    }

    public void remove(String key) {
        keyCache.remove(key);
    }

    public void putReferences(String key, List<String> references) {
        keyChildrenCache.put(key, references);
    }

    public boolean containsKey(String key) {
        return keyCache.containsKey(key);
    }

    public boolean containsReferences(String key) {
        return keyChildrenCache.containsKey(key);
    }

    public void removeReference(String parentId, String childId) {
        List<String> references = getReferences(parentId);
        if (references == null) return; // ignore ?

        references.remove(childId);

        if (references.size() == 0)
            keyChildrenCache.remove(parentId); // drop key with no references
    }
}
