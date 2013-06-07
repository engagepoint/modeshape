package org.modeshape.connector.cmis.util;

import org.infinispan.schematic.document.Document;
import org.modeshape.connector.cmis.MappedCustomType;
import org.modeshape.connector.cmis.MappedTypesContainer;
import org.modeshape.connector.cmis.config.TypeCustomMappingList;

public class TypeMappingConfigUtil {

    public static String PROPERTY_JCR_TYPE_NAME = "jcrName";
    public static String PROPERTY_EXTERNAL_TYPE_NAME = "extName";
    public static String PROPERTY_MAPPED_PROPERTIES = "propertyMappings";
    public static String PROPERTY_JCR_NAMESPACE = "jcrNamespaceUri";

    public static MappedTypesContainer getMappedTypes(TypeCustomMappingList typeMappings) {
        MappedTypesContainer result = new MappedTypesContainer();
        if (typeMappings == null) return result;
        for (Object obja : typeMappings.getTypes()) {
            Document typeMapping = (Document) obja;

            String jcrType = typeMapping.getString(PROPERTY_JCR_TYPE_NAME);
            String externalType = typeMapping.getString(PROPERTY_EXTERNAL_TYPE_NAME);

            MappedCustomType mappedCustomType = new MappedCustomType(jcrType, externalType);
            mappedCustomType.setJcrNamespaceUri(typeMapping.getString(PROPERTY_JCR_NAMESPACE));

            if (typeMapping.getDocument(PROPERTY_MAPPED_PROPERTIES) != null) {
                Iterable<Document.Field> propertyMappings = typeMapping.getDocument(PROPERTY_MAPPED_PROPERTIES).fields();
                for (Document.Field propertyMapping : propertyMappings) {
//                    System.out.println("Prop mapping: " + propertyMapping.getName() + " = " + propertyMapping.getValueAsString());
                    mappedCustomType.addPropertyMapping(propertyMapping.getName(), propertyMapping.getValueAsString());
                }
            }

            result.addTypeMapping(mappedCustomType);
        }

        return result;
    }
}
