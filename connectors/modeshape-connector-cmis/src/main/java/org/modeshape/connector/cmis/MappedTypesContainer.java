package org.modeshape.connector.cmis;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class MappedTypesContainer {

    List<MappedCustomType> mappings = new LinkedList<MappedCustomType>();

    Map<String, MappedCustomType> indexByJcrName = new HashMap<String, MappedCustomType>();
    Map<String, MappedCustomType> indexByExtName = new HashMap<String, MappedCustomType>();

    public void addTypeMapping(MappedCustomType customType) {
        mappings.add(customType);
        indexByJcrName.put(customType.getJcrName(), customType);
        indexByExtName.put(customType.getExtName(), customType);
    }


    public MappedCustomType findByJcrName(String value) {
        return indexByJcrName.get(value);
    }

    public MappedCustomType findByExtName(String value) {
        return indexByExtName.get(value);
    }

    public boolean isEmpty() {
        return mappings.isEmpty();
    }

    public int size() {
        return mappings != null ? mappings.size() : 0;
    }
}
