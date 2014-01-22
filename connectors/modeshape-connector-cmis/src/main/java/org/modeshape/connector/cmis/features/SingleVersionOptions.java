package org.modeshape.connector.cmis.features;

import org.modeshape.jcr.api.nodetype.NodeTypeManager;
import org.modeshape.jcr.value.Name;
import org.modeshape.jcr.value.ValueFactories;

import java.util.HashSet;
import java.util.Set;

public class SingleVersionOptions {

    String commonIdPropertyName;
    String commonIdTypeName;
    String commonIdQuery;
    private String[] singleVersionTypes;
    private String singleVersionGUIDPrefix = "FAKE_";

    //  common id as child reference
    private boolean useCommonIdAsPrimary = true;

    // working property
    private Set<Name> singleVersionTypeNames = new HashSet<Name>();

    public int initialize(ValueFactories factories, NodeTypeManager nodeTypeManager) {
        if (singleVersionTypes == null) return 0;

        for (String typeName : singleVersionTypes) {
            singleVersionTypeNames.add(factories.getNameFactory().create(typeName));
        }

        return singleVersionTypeNames.size();
    }

    public String getCommonIdPropertyName() {
        return commonIdPropertyName;
    }

    public String getCommonIdTypeName() {
        return commonIdTypeName;
    }

    public String getCommonIdQuery() {
        return commonIdQuery;
    }

    public String getSingleVersionGUIDPrefix() {
        return singleVersionGUIDPrefix;
    }

    public boolean isUseCommonIdAsPrimary() {
        return useCommonIdAsPrimary;
    }

    public Set<Name> getSingleVersionTypeNames() {
        return singleVersionTypeNames;
    }
}
