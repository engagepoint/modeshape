package org.modeshape.connector.cmis.config;

import java.util.Map;

public class CustomTypeMappingConfig {

    private String typeName;
    private String externalTypeName;
    private Map<String, String> propertyMappings;

    public String getTypeName() {
        return typeName;
    }

    public String getExternalTypeName() {
        return externalTypeName;
    }

    public Map<String, String> getPropertyMappings() {
        return propertyMappings;
    }

}
