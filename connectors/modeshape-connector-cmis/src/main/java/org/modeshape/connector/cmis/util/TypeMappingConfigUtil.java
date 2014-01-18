package org.modeshape.connector.cmis.util;

import org.infinispan.schematic.document.Document;
import org.modeshape.connector.cmis.mapping.MappedCustomType;
import org.modeshape.connector.cmis.mapping.MappedTypesContainer;
import org.modeshape.connector.cmis.config.TypeCustomMappingList;

import java.util.List;

public class TypeMappingConfigUtil {

    public static String PROPERTY_JCR_TYPE_NAME = "jcrName";
    public static String PROPERTY_EXTERNAL_TYPE_NAME = "extName";
    public static String PROPERTY_MAPPED_PROPERTIES = "propertyMappings";
    public static String PROPERTY_IGNORE_EXTERNAL_PROPERTIES = "ignoreExternalProperties";
    public static String PROPERTY_JCR_NAMESPACE = "jcrNamespaceUri";
    public static String PROPERTY_JCR_FEATURES = "propertyFeatures";

    public static MappedTypesContainer getMappedTypes(TypeCustomMappingList typeMappings, MappedCustomType defaultsDelegate) {
        MappedTypesContainer result = new MappedTypesContainer(defaultsDelegate);
        if (typeMappings == null) return result;
        for (Object obja : typeMappings.getTypes()) {
            Document typeMapping = (Document) obja;

            String jcrType = typeMapping.getString(PROPERTY_JCR_TYPE_NAME);
            String externalType = typeMapping.getString(PROPERTY_EXTERNAL_TYPE_NAME);

            MappedCustomType mappedCustomType = new MappedCustomType(jcrType, externalType, defaultsDelegate);
            mappedCustomType.setJcrNamespaceUri(typeMapping.getString(PROPERTY_JCR_NAMESPACE));

            if (typeMapping.getDocument(PROPERTY_MAPPED_PROPERTIES) != null) {
                Iterable<Document.Field> propertyMappings = typeMapping.getDocument(PROPERTY_MAPPED_PROPERTIES).fields();
                for (Document.Field propertyMapping : propertyMappings) {
                    mappedCustomType.addPropertyMapping(propertyMapping.getName(), propertyMapping.getValueAsString());
                }
            }
            
            List<String> ignoreExternalProperties = (List<String>) typeMapping.getArray(PROPERTY_IGNORE_EXTERNAL_PROPERTIES);
            if (ignoreExternalProperties != null && ignoreExternalProperties.size() > 0) {
            	mappedCustomType.setIgnoreExternalProperties(ignoreExternalProperties);
            }

            Document features = typeMapping.getDocument(PROPERTY_JCR_FEATURES);
            if (features != null) {
                Iterable<Document.Field> featuresList = features.fields();
                for (Document.Field feature : featuresList) {
                    mappedCustomType.updatePropertyFeature(feature.getName(), feature.getValueAsString());
                }
            }
            result.addTypeMapping(mappedCustomType);
        }

        return result;
    }

}
