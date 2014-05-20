package org.modeshape.connector.cmis.util;

import org.apache.chemistry.opencmis.client.api.ObjectType;
import org.apache.chemistry.opencmis.commons.definitions.Choice;
import org.apache.chemistry.opencmis.commons.definitions.DocumentTypeDefinition;
import org.apache.chemistry.opencmis.commons.definitions.PropertyDefinition;
import org.apache.commons.collections.CollectionUtils;
import org.modeshape.common.collection.Problems;
import org.modeshape.common.collection.SimpleProblems;
import org.modeshape.common.i18n.I18n;
import org.modeshape.connector.cmis.common.CheckTypesI18n;
import org.modeshape.connector.cmis.common.TypeDefinitionsIds;

import java.util.*;

/**
 * Self Check Type synchronization Util
 * Created by vyacheslav.polulyakh on 5/14/2014.
 */
public class CheckTypeSynchronizationUtil {

    private static final String ADDED = "Added";
    private static final String DELETED = "Deleted";
    private static final String PARAMETER_FROM_TO = "parameter %s from %s to: %s";
    private static final String FROM_TO = "from: %s to %s";

    protected static Problems problems;

    public static Problems checkTypeDefinitions(Map<String, ObjectType> expectedTypes, Map<String, ObjectType> actualTypes) {
        problems = new SimpleProblems();

        if (isNullValues(expectedTypes, actualTypes)) {
            return problems;
        }

        expectedTypes = compareMaps(expectedTypes, actualTypes, CheckTypesI18n.typeWas, null);

        for (Map.Entry<String, ObjectType> entry : expectedTypes.entrySet()) {

            if (entry.getValue().isBaseType()) {
                continue;
            }
            compareObjectType(entry.getValue(), actualTypes.get(entry.getKey()));
        }
        return problems;
    }

    protected static void compareObjectType(ObjectType expectedType, ObjectType actualType) {

        if (isNullValues(expectedType, actualType)) {
            return;
        }

        String typeId = expectedType.getId();

        compareForErrorObjectTypeFields(expectedType.getDisplayName(), actualType.getDisplayName(), typeId, TypeDefinitionsIds.DISPLAY_NAME);
        compareForErrorObjectTypeFields(expectedType.getLocalName(), actualType.getLocalName(), typeId, TypeDefinitionsIds.LOCAL_NAME);
        compareForErrorObjectTypeFields(expectedType.getLocalNamespace(), actualType.getLocalNamespace(), typeId, TypeDefinitionsIds.LOCAL_NAMESPACE);
        compareForErrorObjectTypeFields(expectedType.getQueryName(), actualType.getQueryName(), typeId, TypeDefinitionsIds.QUERY_NAME);
        compareForErrorObjectTypeFields(expectedType.getParentTypeId(), actualType.getParentTypeId(), typeId, TypeDefinitionsIds.PARENT_TYPE);
        compareForWarningObjectTypeFields(expectedType.getDescription(), actualType.getDescription(), typeId, TypeDefinitionsIds.DESCRIPTION);

        compareForErrorObjectTypeFields(expectedType.isQueryable(), actualType.isQueryable(), typeId, TypeDefinitionsIds.IS_QUERYABLE);
        compareForWarningObjectTypeFields(expectedType.isControllableAcl(), actualType.isControllableAcl(), typeId, TypeDefinitionsIds.IS_CONTROLLABLE_ACL);
        compareForWarningObjectTypeFields(expectedType.isControllablePolicy(), actualType.isControllablePolicy(), typeId, TypeDefinitionsIds.IS_CONTROLLABLE_POLICY);
        compareForErrorObjectTypeFields(expectedType.isCreatable(), actualType.isCreatable(), typeId, TypeDefinitionsIds.IS_CREATABLE);
        compareForErrorObjectTypeFields(expectedType.isFileable(), actualType.isFileable(), typeId, TypeDefinitionsIds.IS_FILEABLE);
        compareForErrorObjectTypeFields(expectedType.isFulltextIndexed(), actualType.isFulltextIndexed(), typeId, TypeDefinitionsIds.IS_FULLTEXT_INDEXED);
        compareForErrorObjectTypeFields(expectedType.isIncludedInSupertypeQuery(), actualType.isIncludedInSupertypeQuery(), typeId, TypeDefinitionsIds.IS_INCLUDED_IN_SUPERTYPE_QUERY);

        if (expectedType instanceof DocumentTypeDefinition && actualType instanceof DocumentTypeDefinition) {
            compareForErrorObjectTypeFields(((DocumentTypeDefinition) expectedType).isVersionable(), ((DocumentTypeDefinition) actualType).isVersionable(), typeId, TypeDefinitionsIds.IS_VERSIONABLE);
        }

        //TODO check Extensions

        comparePropertyDefinitions(expectedType.getPropertyDefinitions(), actualType.getPropertyDefinitions(), typeId);
    }

    protected static void compareForErrorObjectTypeFields(Object expected, Object actual, String typeId, String fieldName) {
        if (!compareValues(expected, actual)) {
            problems.addError(CheckTypesI18n.typeAreChanged, typeId, String.format(PARAMETER_FROM_TO, fieldName, expected, actual));
        }
    }

    protected static void compareForWarningObjectTypeFields(Object expected, Object actual, String typeId, String fieldName) {
        if (!compareValues(expected, actual)) {
            problems.addWarning(CheckTypesI18n.typeAreChanged, typeId, String.format(PARAMETER_FROM_TO, fieldName, expected, actual));
        }
    }

    protected static void comparePropertyDefinitions(Map<String, PropertyDefinition<?>> expectedProperties, Map<String, PropertyDefinition<?>> actualProperties, String typeId) {

        if (isNullValues(expectedProperties, actualProperties)) {
            return;
        }

        expectedProperties = compareMaps(expectedProperties, actualProperties, CheckTypesI18n.propertyAreChanged, typeId);

        for (Map.Entry<String, PropertyDefinition<?>> entry : expectedProperties.entrySet()) {
            compareProperty(entry.getValue(), actualProperties.get(entry.getKey()), typeId);
        }
    }

    protected static void compareProperty(PropertyDefinition<?> expectedProperty, PropertyDefinition<?> actualProperty, String typeId) {

        if (isNullValues(problems, expectedProperty, actualProperty)) {
            return;
        }

        String propertyId = expectedProperty.getId();

        compareForErrorPropertyFields(expectedProperty.getDisplayName(), actualProperty.getDisplayName(), typeId, propertyId, TypeDefinitionsIds.DISPLAY_NAME);
        compareForErrorPropertyFields(expectedProperty.getLocalNamespace(), actualProperty.getLocalNamespace(), typeId, propertyId, TypeDefinitionsIds.LOCAL_NAMESPACE);
        compareForErrorPropertyFields(expectedProperty.getLocalName(), actualProperty.getLocalName(), typeId, propertyId, TypeDefinitionsIds.LOCAL_NAME);
        compareForErrorPropertyFields(expectedProperty.getQueryName(), actualProperty.getQueryName(), typeId, propertyId, TypeDefinitionsIds.QUERY_NAME);
        compareForWarningPropertyFields(expectedProperty.getDescription(), actualProperty.getDescription(), typeId, propertyId, TypeDefinitionsIds.DESCRIPTION);
        compareForErrorPropertyFields(expectedProperty.getCardinality(), actualProperty.getCardinality(), typeId, propertyId, TypeDefinitionsIds.CARDINALITY);
        compareForErrorPropertyFields(expectedProperty.getPropertyType(), actualProperty.getPropertyType(), typeId, propertyId, TypeDefinitionsIds.PROPERTY_TYPE);

        compareForErrorPropertyFields(expectedProperty.isInherited(), actualProperty.isInherited(), typeId, propertyId, TypeDefinitionsIds.IS_INHERITED);
        compareForErrorPropertyFields(expectedProperty.isOpenChoice(), actualProperty.isOpenChoice(), typeId, propertyId, TypeDefinitionsIds.IS_OPEN_CHOICE);
        compareForErrorPropertyFields(expectedProperty.isOrderable(), actualProperty.isOrderable(), typeId, propertyId, TypeDefinitionsIds.IS_ORDERABLE);
        compareForErrorPropertyFields(expectedProperty.isQueryable(), actualProperty.isQueryable(), typeId, propertyId, TypeDefinitionsIds.IS_QUERYABLE);
        compareForErrorPropertyFields(expectedProperty.isRequired(), actualProperty.isRequired(), typeId, propertyId, TypeDefinitionsIds.IS_REQUIRED);
        compareForErrorPropertyFields(expectedProperty.getUpdatability(), actualProperty.getUpdatability(), typeId, propertyId, TypeDefinitionsIds.UPDATABILITY);


        List<?> expectedDefaultValue = expectedProperty.getDefaultValue();
        List<?> actualDefaultValue = actualProperty.getDefaultValue();

        expectedDefaultValue = expectedDefaultValue == null ? Collections.emptyList() : expectedDefaultValue;
        actualDefaultValue = actualDefaultValue == null ? Collections.emptyList() : actualDefaultValue;

/*        boolean isAllNulls = expectedProperty.getDefaultValue() == null && actualProperty.getDefaultValue() == null;
        boolean isNullValues = expectedProperty.getDefaultValue() == null || actualProperty.getDefaultValue() == null;

        boolean isEquals = !isNullValues && CollectionUtils.isEqualCollection(expectedProperty.getDefaultValue(), actualProperty.getDefaultValue());

        if (!(isAllNulls || isEquals)) {*/

        if (!CollectionUtils.isEqualCollection(expectedDefaultValue,actualDefaultValue)) {
            problems.addWarning(CheckTypesI18n.propertyAreChanged, typeId, propertyId,
                    String.format(PARAMETER_FROM_TO, TypeDefinitionsIds.DEFAULT_VALUE, expectedProperty.getDefaultValue(), actualProperty.getDefaultValue()));
        }

        //TODO check

        compareChoices(expectedProperty.getChoices(), actualProperty.getChoices(), typeId, propertyId);
    }

    protected static void compareForErrorPropertyFields(Object expected, Object actual, String typeId, String propertyId, String fieldName) {
        if (!compareValues(expected, actual)) {
            problems.addError(CheckTypesI18n.propertyAreChanged, typeId, propertyId, String.format(PARAMETER_FROM_TO, fieldName, expected, actual));
        }
    }

    protected static void compareForWarningPropertyFields(Object expected, Object actual, String typeId, String propertyId, String fieldName) {
        if (!compareValues(expected, actual)) {
            problems.addWarning(CheckTypesI18n.propertyAreChanged, typeId, propertyId, String.format(PARAMETER_FROM_TO, fieldName, expected, actual));
        }
    }

    protected static void compareChoices(List<? extends Choice<?>> expectedChoices, List<? extends Choice<?>> actualChoices, String typeId, String propertyId) {

        if (expectedChoices.isEmpty() && actualChoices.isEmpty()) {
            return;
        }

        if (expectedChoices.size() != actualChoices.size()) {
            problems.addError(CheckTypesI18n.choiceAreChanged, typeId, propertyId, "Choices are changed");
            return;
        }

        //TODO
        for (int i = 0; i < expectedChoices.size(); i++) {
            compareChoice(expectedChoices.get(i), actualChoices.get(i), typeId, propertyId);
        }
    }

    protected static void compareChoice(Choice<?> expectedChoice, Choice<?> actualChoice, String typeId, String propertyId) {

        if (!compareValues(expectedChoice.getDisplayName(), actualChoice.getDisplayName())) {
            problems.addError(CheckTypesI18n.choiceAreChanged, typeId, propertyId, TypeDefinitionsIds.DISPLAY_NAME);
        }

        if (!CollectionUtils.isEqualCollection(expectedChoice.getValue(), actualChoice.getValue())) {
            problems.addError(CheckTypesI18n.choiceAreChanged, typeId, propertyId, String.format(FROM_TO, expectedChoice.getValue().toString(), actualChoice.getValue().toString()));
        }
    }

    protected static <T> Map<String, T> compareMaps(Map<String, T> expected, Map<String, T> actual, I18n message, String param) {
        Map<String, T> expectedMap = new HashMap<String, T>(expected);
        Set<String> expectedKeys = new HashSet<String>(expectedMap.keySet());
        Set<String> actualKeys = new HashSet<String>(actual.keySet());

        boolean isEqualsLength = actualKeys.size() == expectedKeys.size();
        boolean isEqualsKeys = actualKeys.containsAll(expectedKeys) && expectedKeys.containsAll(actualKeys);

        if (!(isEqualsLength && isEqualsKeys)) {

            actualKeys.removeAll(expectedMap.keySet());
            expectedKeys.removeAll(actual.keySet());

            if (!expectedKeys.isEmpty()) {
                for (String key : expectedKeys) {
                    problems.addError(message, key, DELETED, param);
                    expectedMap.remove(key);
                }
            }

            if (!actualKeys.isEmpty()) {
                for (String key : actualKeys) {
                    problems.addWarning(message, key, ADDED, param);
                }
            }
        }
        return expectedMap;
    }

    protected static boolean isNullValues(Object... objects) {

        for (Object o : objects) {
            if (o == null) {
                problems.addError(CheckTypesI18n.argumentMayNotBeNull);
                return true;
            }
        }
        return false;
    }

    protected static boolean compareValues(Object expected, Object actual) {

        boolean isNulls = expected == null && actual == null;
        boolean isEquals = expected != null && expected.equals(actual);

        return isNulls || isEquals;
    }
}
