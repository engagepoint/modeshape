package org.modeshape.connector.cmis.util;

import org.apache.chemistry.opencmis.client.api.ObjectType;
import org.apache.chemistry.opencmis.commons.definitions.Choice;
import org.apache.chemistry.opencmis.commons.definitions.DocumentTypeDefinition;
import org.apache.chemistry.opencmis.commons.definitions.PropertyDefinition;
import org.apache.commons.collections.CollectionUtils;
import org.modeshape.common.collection.Problems;
import org.modeshape.common.collection.SimpleProblems;
import org.modeshape.common.i18n.I18n;
import org.modeshape.connector.cmis.common.CompareTypesI18n;
import org.modeshape.connector.cmis.common.TypeDefinitionsIds;

import java.util.*;

/**
 * Self Check Type conformity Util
 * Created by vyacheslav.polulyakh on 5/14/2014.
 */
public final class CompareTypeDefinitionsUtil {

    private CompareTypeDefinitionsUtil(){}

    private static final String ADDED_TO = "added to ";
    private static final String REMOVED_FROM = "removed from";
    private static final String PARAMETER_FROM_TO = "parameter %s from %s to: %s";
    private static final String FROM_TO = "from %s to %s";
    private static final String TYPE_DEFINITIONS = "Type Definitions";

    protected static Problems problems;

    /**
     * Compare two Types Definitions and add <code>Error</code> or <code>Warning</code> to returned {@link org.modeshape.common.collection.Problems}
     * if it's has discrepancy
     * @return {@link org.modeshape.common.collection.Problems} witch contains all found discrepancy
     */
    public static Problems compareTypeDefinitions(Map<String, ObjectType> expectedTypes, Map<String, ObjectType> actualTypes) {
        problems = new SimpleProblems();

        if (isNullValues(expectedTypes, actualTypes)) {
            return problems;
        }

        expectedTypes = compareMaps(expectedTypes, actualTypes, CompareTypesI18n.typeWas, TYPE_DEFINITIONS);

        for (Map.Entry<String, ObjectType> entry : expectedTypes.entrySet()) {

            if (entry.getValue().isBaseType()) {
                continue;
            }
            compareObjectType(entry.getValue(), actualTypes.get(entry.getKey()));
        }
        return problems;
    }

    /**
     * Compare two {@link org.apache.chemistry.opencmis.client.api.ObjectType} to equals,
     * and add <code>Error</code> or <code>Warning</code> to {@link #problems}
     * if it's fields and {@link org.apache.chemistry.opencmis.commons.definitions.PropertyDefinition}(s) not equals
     * used method {@link #comparePropertyDefinitions(java.util.Map, java.util.Map, String)} for compare it's properties
     */
    protected static void compareObjectType(ObjectType expectedType, ObjectType actualType) {

        if (isNullValues(expectedType, actualType)) {
            return;
        }

        String typeId = expectedType.getId();

        compareForWarningObjectTypeFields(expectedType.getDisplayName(), actualType.getDisplayName(), typeId, TypeDefinitionsIds.DISPLAY_NAME);
        compareForErrorObjectTypeFields(expectedType.getLocalName(), actualType.getLocalName(), typeId, TypeDefinitionsIds.LOCAL_NAME);
        compareForErrorObjectTypeFields(expectedType.getLocalNamespace(), actualType.getLocalNamespace(), typeId, TypeDefinitionsIds.LOCAL_NAMESPACE);
        compareForErrorObjectTypeFields(expectedType.getQueryName(), actualType.getQueryName(), typeId, TypeDefinitionsIds.QUERY_NAME);
        compareForErrorObjectTypeFields(expectedType.getParentTypeId(), actualType.getParentTypeId(), typeId, TypeDefinitionsIds.PARENT_TYPE);
        compareForWarningObjectTypeFields(expectedType.getDescription(), actualType.getDescription(), typeId, TypeDefinitionsIds.DESCRIPTION);

        compareForWarningObjectTypeFields(expectedType.isQueryable(), actualType.isQueryable(), typeId, TypeDefinitionsIds.IS_QUERYABLE);
        compareForWarningObjectTypeFields(expectedType.isControllableAcl(), actualType.isControllableAcl(), typeId, TypeDefinitionsIds.IS_CONTROLLABLE_ACL);
        compareForWarningObjectTypeFields(expectedType.isControllablePolicy(), actualType.isControllablePolicy(), typeId, TypeDefinitionsIds.IS_CONTROLLABLE_POLICY);
        compareForErrorObjectTypeFields(expectedType.isCreatable(), actualType.isCreatable(), typeId, TypeDefinitionsIds.IS_CREATABLE);
        compareForWarningObjectTypeFields(expectedType.isFileable(), actualType.isFileable(), typeId, TypeDefinitionsIds.IS_FILEABLE);
        compareForWarningObjectTypeFields(expectedType.isFulltextIndexed(), actualType.isFulltextIndexed(), typeId, TypeDefinitionsIds.IS_FULLTEXT_INDEXED);
        compareForWarningObjectTypeFields(expectedType.isIncludedInSupertypeQuery(), actualType.isIncludedInSupertypeQuery(), typeId, TypeDefinitionsIds.IS_INCLUDED_IN_SUPERTYPE_QUERY);

        if (expectedType instanceof DocumentTypeDefinition && actualType instanceof DocumentTypeDefinition) {
            compareForWarningObjectTypeFields(((DocumentTypeDefinition) expectedType).isVersionable(), ((DocumentTypeDefinition) actualType).isVersionable(), typeId, TypeDefinitionsIds.IS_VERSIONABLE);
        }

        comparePropertyDefinitions(expectedType.getPropertyDefinitions(), actualType.getPropertyDefinitions(), typeId);
    }

    /**
     * Compare two value of fields from {@link org.apache.chemistry.opencmis.client.api.ObjectType} to equals,
     * and add <code>Error</code> to {@link #problems} if it's not equals
     * @param expected value
     * @param actual value
     * @param typeId Id of type where are this field
     * @param fieldName name of this field
     */
    protected static void compareForErrorObjectTypeFields(Object expected, Object actual, String typeId, String fieldName) {
        if (!compareValues(expected, actual)) {
            problems.addError(CompareTypesI18n.typeAreChanged, fieldName, typeId, expected, actual);
        }
    }

    /**
     * Compare two value of fields from {@link org.apache.chemistry.opencmis.client.api.ObjectType} to equals,
     * and add <code>Warning</code> to {@link #problems} if it's not equals
     * @param expected value
     * @param actual value
     * @param typeId Id of type where are this field
     * @param fieldName name of this field
     */
    protected static void compareForWarningObjectTypeFields(Object expected, Object actual, String typeId, String fieldName) {
        if (!compareValues(expected, actual)) {
            problems.addWarning(CompareTypesI18n.typeAreChanged, fieldName, typeId, expected, actual);
        }
    }

    /**
     * Compare two {@link Map} of {@link org.apache.chemistry.opencmis.commons.definitions.PropertyDefinition} to equals,
     * use method {@link #compareMaps(java.util.Map, java.util.Map, org.modeshape.common.i18n.I18n, String)} and {@link #compareProperty} for it
     * @param typeId Id of type where are this properties
     */
    protected static void comparePropertyDefinitions(Map<String, PropertyDefinition<?>> expectedProperties, Map<String, PropertyDefinition<?>> actualProperties, String typeId) {

        if (isNullValues(expectedProperties, actualProperties)) {
            return;
        }

        expectedProperties = compareMaps(expectedProperties, actualProperties, CompareTypesI18n.propertyWas, typeId);

        for (Map.Entry<String, PropertyDefinition<?>> entry : expectedProperties.entrySet()) {
            compareProperty(entry.getValue(), actualProperties.get(entry.getKey()), typeId);
        }
    }

    /**
     * Compare two {@link org.apache.chemistry.opencmis.commons.definitions.PropertyDefinition} to equals,
     * and add <code>Error</code> or <code>Warning</code> to {@link #problems}
     * if it's fields and {@link org.apache.chemistry.opencmis.commons.definitions.Choice}(s) not equals
     * @param typeId Id of type where are this property
     */
    protected static void compareProperty(PropertyDefinition<?> expectedProperty, PropertyDefinition<?> actualProperty, String typeId) {

        if (isNullValues(problems, expectedProperty, actualProperty)) {
            return;
        }

        String propertyId = expectedProperty.getId();

        compareForWarningPropertyFields(expectedProperty.getDisplayName(), actualProperty.getDisplayName(), typeId, propertyId, TypeDefinitionsIds.DISPLAY_NAME);
        compareForErrorPropertyFields(expectedProperty.getLocalNamespace(), actualProperty.getLocalNamespace(), typeId, propertyId, TypeDefinitionsIds.LOCAL_NAMESPACE);
        compareForErrorPropertyFields(expectedProperty.getLocalName(), actualProperty.getLocalName(), typeId, propertyId, TypeDefinitionsIds.LOCAL_NAME);
        compareForErrorPropertyFields(expectedProperty.getQueryName(), actualProperty.getQueryName(), typeId, propertyId, TypeDefinitionsIds.QUERY_NAME);
        compareForWarningPropertyFields(expectedProperty.getDescription(), actualProperty.getDescription(), typeId, propertyId, TypeDefinitionsIds.DESCRIPTION);
        compareForErrorPropertyFields(expectedProperty.getCardinality(), actualProperty.getCardinality(), typeId, propertyId, TypeDefinitionsIds.CARDINALITY);
        compareForErrorPropertyFields(expectedProperty.getPropertyType(), actualProperty.getPropertyType(), typeId, propertyId, TypeDefinitionsIds.PROPERTY_TYPE);

        compareForWarningPropertyFields(expectedProperty.isInherited(), actualProperty.isInherited(), typeId, propertyId, TypeDefinitionsIds.IS_INHERITED);
        compareForWarningPropertyFields(expectedProperty.isOpenChoice(), actualProperty.isOpenChoice(), typeId, propertyId, TypeDefinitionsIds.IS_OPEN_CHOICE);
        compareForWarningPropertyFields(expectedProperty.isOrderable(), actualProperty.isOrderable(), typeId, propertyId, TypeDefinitionsIds.IS_ORDERABLE);
        compareForWarningPropertyFields(expectedProperty.isQueryable(), actualProperty.isQueryable(), typeId, propertyId, TypeDefinitionsIds.IS_QUERYABLE);
        compareForErrorPropertyFields(expectedProperty.isRequired(), actualProperty.isRequired(), typeId, propertyId, TypeDefinitionsIds.IS_REQUIRED);
        compareForWarningPropertyFields(expectedProperty.getUpdatability(), actualProperty.getUpdatability(), typeId, propertyId, TypeDefinitionsIds.UPDATABILITY);


        List<?> expectedDefaultValue = expectedProperty.getDefaultValue();
        List<?> actualDefaultValue = actualProperty.getDefaultValue();

        expectedDefaultValue = expectedDefaultValue == null ? Collections.emptyList() : expectedDefaultValue;
        actualDefaultValue = actualDefaultValue == null ? Collections.emptyList() : actualDefaultValue;

        if (!CollectionUtils.isEqualCollection(expectedDefaultValue, actualDefaultValue)) {
            problems.addWarning(CompareTypesI18n.propertyAreChanged, TypeDefinitionsIds.DEFAULT_VALUE, propertyId, typeId,
                    expectedProperty.getDefaultValue(), actualProperty.getDefaultValue());
        }

        compareChoices(expectedProperty.getChoices(), actualProperty.getChoices(), typeId, propertyId);
    }

    /**
     * Compare two value of fields from {@link org.apache.chemistry.opencmis.commons.definitions.PropertyDefinition} to equals,
     * and add <code>Error</code> to {@link #problems} if it's not equals
     * @param expected value
     * @param actual value
     * @param typeId Id of type where are this property
     * @param propertyId Id of property where this field are
     * @param fieldName name of this field
     */
    protected static void compareForErrorPropertyFields(Object expected, Object actual, String typeId, String propertyId, String fieldName) {
        if (!compareValues(expected, actual)) {
            problems.addError(CompareTypesI18n.propertyAreChanged, fieldName, propertyId, typeId, expected, actual);
        }
    }

    /**
     * Compare two value of fields from {@link org.apache.chemistry.opencmis.commons.definitions.PropertyDefinition} to equals,
     * and add <code>Warning</code> to {@link #problems} if it's not equals
     * @param expected value
     * @param actual value
     * @param typeId Id of type where are this property
     * @param propertyId Id of property where this field are
     * @param fieldName name of this field
     */
    protected static void compareForWarningPropertyFields(Object expected, Object actual, String typeId, String propertyId, String fieldName) {
        if (!compareValues(expected, actual)) {
            problems.addWarning(CompareTypesI18n.propertyAreChanged, fieldName, propertyId, typeId, expected, actual);
        }
    }

    /**
     * Compare two {@link List} of {@link org.apache.chemistry.opencmis.commons.definitions.Choice}(s)
     * for compare one {@link org.apache.chemistry.opencmis.commons.definitions.Choice} used method {@link #compareChoice(org.apache.chemistry.opencmis.commons.definitions.Choice, org.apache.chemistry.opencmis.commons.definitions.Choice, String, String)}
     * and add <code>Error</code> to {@link #problems} if it's not <code>equals</code>
     * @param expectedChoices expected {@link List} of {@link org.apache.chemistry.opencmis.commons.definitions.Choice}
     * @param actualChoices actual {@link List} of {@link org.apache.chemistry.opencmis.commons.definitions.Choice}
     * @param typeId Id of type where are property witch contain this {@link org.apache.chemistry.opencmis.commons.definitions.Choice}(s)
     * @param propertyId Id of property where this {@link org.apache.chemistry.opencmis.commons.definitions.Choice}(s) are
     */
    protected static void compareChoices(List<? extends Choice<?>> expectedChoices, List<? extends Choice<?>> actualChoices, String typeId, String propertyId) {

        if (expectedChoices.isEmpty() && actualChoices.isEmpty()) {
            return;
        }

        if (expectedChoices.size() != actualChoices.size()) {
            problems.addError(CompareTypesI18n.choiceAreChanged, typeId, propertyId, "Choices are changed");
            return;
        }

        for (int i = 0; i < expectedChoices.size(); i++) {
            compareChoice(expectedChoices.get(i), actualChoices.get(i), typeId, propertyId);
        }
    }

    /**
     * Compare two {@link org.apache.chemistry.opencmis.commons.definitions.Choice}
     * If {@link org.apache.chemistry.opencmis.commons.definitions.Choice} are changed add <code>Error</code>
     * to {@link #problems}
     * @param expectedChoice expected {@link org.apache.chemistry.opencmis.commons.definitions.Choice}
     * @param actualChoice actual {@link org.apache.chemistry.opencmis.commons.definitions.Choice}
     * @param typeId Id of type where are property witch contain this {@link org.apache.chemistry.opencmis.commons.definitions.Choice}
     * @param propertyId Id of property where this {@link org.apache.chemistry.opencmis.commons.definitions.Choice} are
     */
    protected static void compareChoice(Choice<?> expectedChoice, Choice<?> actualChoice, String typeId, String propertyId) {

        if (!compareValues(expectedChoice.getDisplayName(), actualChoice.getDisplayName())) {
            problems.addError(CompareTypesI18n.choiceAreChanged, typeId, propertyId, TypeDefinitionsIds.DISPLAY_NAME);
        }

        if (!CollectionUtils.isEqualCollection(expectedChoice.getValue(), actualChoice.getValue())) {
            problems.addError(CompareTypesI18n.choiceAreChanged, typeId, propertyId, String.format(FROM_TO, expectedChoice.getValue().toString(), actualChoice.getValue().toString()));
        }
    }

    /**
     * Compare two maps for equals it's <code>keys</code>
     * Create a copy of expected map with keys, witch are in actual map and in expected map
     * If keys not exist in actual map, add Error to {@link #problems} with <code>message</code> and parameters <code>deleted</code>, <code>param</code>
     * If keys not exist in expected map, add Warning to {@link #problems} with <code>message</code> and parameters <code>added</code>, <code>param</code>
     * @param message {@link org.modeshape.common.i18n.I18n} message
     * @param param additional info for <code>message</code>
     * @return cope of expected map, without keys witch not exist in actual map
     */
    protected static <T> Map<String, T> compareMaps(Map<String, T> expected, Map<String, T> actual, I18n message, String param) {
        Map<String, T> expectedMap = new HashMap<String, T>(expected);
        Set<String> expectedKeys = new HashSet<String>(expectedMap.keySet());
        Set<String> actualKeys = new HashSet<String>(actual.keySet());

        boolean isEqualsLength = actualKeys.size() == expectedKeys.size();
        boolean isEqualsKeys = actualKeys.containsAll(expectedKeys) && expectedKeys.containsAll(actualKeys);

        if (!(isEqualsLength && isEqualsKeys)) {

            actualKeys.removeAll(expectedMap.keySet());
            expectedKeys.removeAll(actual.keySet());

            removeDeletedAndAddError(expectedKeys, expectedMap, message, param);
            addProblemIfAdded(actualKeys, actual, message, param);
        }
        return expectedMap;
    }

    private static <T> void removeDeletedAndAddError(Set<String> expectedKeys, Map<String, T> expectedMap,  I18n message, String param) {
        if (!expectedKeys.isEmpty()) {
            for (String key : expectedKeys) {
                problems.addError(message, key, REMOVED_FROM, param);
                expectedMap.remove(key);
            }
        }
    }

    private static  <T> void addProblemIfAdded(Set<String> actualKeys, Map<String, T> actual, I18n message, String param){
        if (!actualKeys.isEmpty()) {
            for (String key : actualKeys) {
                if (isRequiredProperty(actual.get(key))) {
                    problems.addError(message, key, ADDED_TO, param);
                } else {
                    problems.addWarning(message, key, ADDED_TO, param);
                }
            }
        }
    }

    private static boolean isRequiredProperty(Object o){
        if (o instanceof PropertyDefinition){
            if (((PropertyDefinition) o).isRequired())
                return true;
        }
        return false;
    }

    /**
     * Verify all objects for <code>null</code> value, if one of it are <code>null</code>
     * add to {@link #problems} Error {@link org.modeshape.connector.cmis.common.CompareTypesI18n#argumentShouldNotBeNull}
     * @param objects objects to verify
     * @return <code>false</code> if all of objects are not null, else <code>true</code>
     */
    protected static boolean isNullValues(Object... objects) {

        for (Object o : objects) {
            if (o == null) {
                problems.addError(CompareTypesI18n.argumentShouldNotBeNull);
                return true;
            }
        }
        return false;
    }

    /**
     * Compare two object to equals
     * @param expected expected value, may be null
     * @param actual actual value, may be null
     * @return <code>true</code> if object equals or both are null's, else <code>false</code>
     */
    protected static boolean compareValues(Object expected, Object actual) {

        boolean isNulls = expected == null && actual == null;
        boolean isEquals = expected != null && expected.equals(actual);

        return isNulls || isEquals;
    }
}
