package org.modeshape.connector.cmis.mapping;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class MappedTypesContainer {

    List<MappedCustomType> mappings = new LinkedList<MappedCustomType>();

    Map<String, MappedCustomType> indexByJcrName = new HashMap<String, MappedCustomType>();
    Map<String, MappedCustomType> indexByExtName = new HashMap<String, MappedCustomType>();

    MappedCustomType defaultDelegate;

    public MappedTypesContainer(MappedCustomType defaultDelegate) {
        this.defaultDelegate = defaultDelegate;
    }

    public void addTypeMapping(MappedCustomType customType) {
        mappings.add(customType);
        indexByJcrName.put(customType.getJcrName(), customType);
        indexByExtName.put(customType.getExtName(), customType);
    }

    /*
    * add another access point for an already added mapping
    */
    public void addSecondaryJcrKey(String jcrKey, MappedCustomType customType) {
        if (!mappings.contains(customType)) {
            addTypeMapping(customType);
        }
        indexByJcrName.put(jcrKey, customType);
    }


    public MappedCustomType findByJcrName(String value) {
        MappedCustomType result = indexByJcrName.get(value);
        if (result == null) {
            result = new MappedCustomType(value, defaultDelegate);
        }

        return result;
    }

    public MappedCustomType findByExtName(String value) {
        MappedCustomType result = indexByExtName.get(value);
        if (result == null) {
            result = new MappedCustomType(value, defaultDelegate);
        }

        return result;
    }

    public boolean isEmpty() {
        return mappings.isEmpty();
    }

    public int size() {
        return mappings != null ? mappings.size() : 0;
    }


}
