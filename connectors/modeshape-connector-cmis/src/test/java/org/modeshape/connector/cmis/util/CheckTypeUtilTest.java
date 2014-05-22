package org.modeshape.connector.cmis.util;

import org.apache.chemistry.opencmis.client.api.ObjectType;
import org.apache.chemistry.opencmis.client.runtime.objecttype.DocumentTypeImpl;
import org.apache.chemistry.opencmis.commons.definitions.Choice;
import org.apache.chemistry.opencmis.commons.definitions.PropertyDefinition;
import org.apache.chemistry.opencmis.commons.enums.BaseTypeId;
import org.apache.chemistry.opencmis.commons.enums.Cardinality;
import org.apache.chemistry.opencmis.commons.enums.PropertyType;
import org.apache.chemistry.opencmis.commons.enums.Updatability;
import org.junit.Test;
import org.modeshape.common.collection.Problems;
import org.modeshape.common.collection.SimpleProblems;
import org.modeshape.connector.cmis.common.CheckTypesI18n;
import org.modeshape.connector.cmis.common.TypeDefinitionsIds;

import java.util.*;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.*;

/**
 * JUnit Test for {@link CheckTypeUtil}
 * Created by vyacheslav.polulyakh on 5/15/2014.
 */
public class CheckTypeUtilTest {

    @Test
    public void shouldCompareNullsValuesTest() {

        assertTrue(CheckTypeUtil.compareValues("string", "string"));
        assertTrue(CheckTypeUtil.compareValues(null, null));
        assertFalse(CheckTypeUtil.compareValues(null, "string"));
        assertFalse(CheckTypeUtil.compareValues("string", null));
    }

    @Test
    public void shouldVerifyIsNullValuesTest() {

        CheckTypeUtil.problems = new SimpleProblems();

        assertTrue(CheckTypeUtil.isNullValues(null, null));
        assertTrue(CheckTypeUtil.problems.errorCount() == 1);

        assertTrue(CheckTypeUtil.isNullValues("string", "string", null));
        assertTrue(CheckTypeUtil.problems.errorCount() == 2);

        assertFalse(CheckTypeUtil.isNullValues("string", "string"));
        assertTrue(CheckTypeUtil.problems.errorCount() == 2);
    }

    @Test
    public void shouldCompareMapsTest() {
        CheckTypeUtil.problems = new SimpleProblems();
        Map<String, String> expectedMap = new HashMap<String, String>();
        Map<String, String> actualMap = new HashMap<String, String>();

        expectedMap.put("key1", "string");
        actualMap.put("key1", "string");
        CheckTypeUtil.compareMaps(expectedMap, actualMap, CheckTypesI18n.typeWas, null);
        assertFalse(CheckTypeUtil.problems.hasErrors());
        assertTrue(expectedMap.size() == 1);

        expectedMap.put("key2", "string2");
        expectedMap = CheckTypeUtil.compareMaps(expectedMap, actualMap, CheckTypesI18n.typeWas, null);
        assertTrue(CheckTypeUtil.problems.errorCount() == 1);
        assertTrue(CheckTypeUtil.problems.warningCount() == 0);
        assertTrue(expectedMap.size() == 1);
        assertTrue(CheckTypeUtil.problems.iterator().next().getMessage().equals(CheckTypesI18n.typeWas));

        actualMap.put("key2", "string2");
        expectedMap = CheckTypeUtil.compareMaps(expectedMap, actualMap, CheckTypesI18n.propertyWas, "type1");
        assertTrue(CheckTypeUtil.problems.errorCount() == 1);
        assertTrue(CheckTypeUtil.problems.warningCount() == 1);
        assertTrue(expectedMap.size() == 1);
    }

    @Test
    public void shouldCheckChoice() {
        CheckTypeUtil.problems = new SimpleProblems();
        Choice expectedChoice = mock(Choice.class);
        Choice actualChoice = mock(Choice.class);

        when(expectedChoice.getDisplayName()).thenReturn("");
        when(actualChoice.getDisplayName()).thenReturn("");
        when(expectedChoice.getValue()).thenReturn(Collections.emptyList());
        when(actualChoice.getValue()).thenReturn(Collections.emptyList());
        CheckTypeUtil.compareChoice(expectedChoice, actualChoice, "type", "property");
        assertFalse(CheckTypeUtil.problems.hasErrors());

        when(expectedChoice.getValue()).thenReturn(Arrays.asList("blue"));
        when(actualChoice.getValue()).thenReturn(Arrays.asList("blue"));
        CheckTypeUtil.compareChoice(expectedChoice, actualChoice, "type", "property");
        assertFalse(CheckTypeUtil.problems.hasErrors());

        when(expectedChoice.getValue()).thenReturn(Arrays.asList("blue", "red"));
        when(actualChoice.getValue()).thenReturn(Arrays.asList("blue", "red"));
        CheckTypeUtil.compareChoice(expectedChoice, actualChoice, "type", "property");
        assertFalse(CheckTypeUtil.problems.hasErrors());

        when(expectedChoice.getValue()).thenReturn(Arrays.asList("blue", "red"));
        when(actualChoice.getValue()).thenReturn(Arrays.asList("blue"));
        CheckTypeUtil.compareChoice(expectedChoice, actualChoice, "type", "property");
        assertTrue(CheckTypeUtil.problems.errorCount() == 1);

        when(expectedChoice.getValue()).thenReturn(Arrays.asList("blue", "red"));
        when(actualChoice.getValue()).thenReturn(Arrays.asList("blue", "red2"));
        CheckTypeUtil.compareChoice(expectedChoice, actualChoice, "type", "property");
        assertTrue(CheckTypeUtil.problems.errorCount() == 2);

        when(expectedChoice.getValue()).thenReturn(Arrays.asList(5, 7));
        when(actualChoice.getValue()).thenReturn(Arrays.asList(5, 7));
        CheckTypeUtil.compareChoice(expectedChoice, actualChoice, "type", "property");
        assertTrue(CheckTypeUtil.problems.errorCount() == 2);

        when(expectedChoice.getValue()).thenReturn(Arrays.asList("blue", "red"));
        when(actualChoice.getValue()).thenReturn(Collections.emptyList());
        CheckTypeUtil.compareChoice(expectedChoice, actualChoice, "type", "property");
        assertTrue(CheckTypeUtil.problems.errorCount() == 3);
    }

    @Test
    public void shouldCheckChoicesTest() {
        CheckTypeUtil.problems = new SimpleProblems();
        Choice expectedChoice = mock(Choice.class);
        when(expectedChoice.getDisplayName()).thenReturn("");
        Choice expectedChoice2 = mock(Choice.class);
        when(expectedChoice2.getDisplayName()).thenReturn("new");
        Choice actualChoice = mock(Choice.class);
        when(actualChoice.getDisplayName()).thenReturn("");
        Choice actualChoice2 = mock(Choice.class);
        when(actualChoice2.getDisplayName()).thenReturn("new");


        List<Choice<?>> expectedChoices = new ArrayList<Choice<?>>();
        List<Choice<?>> actualChoices = new ArrayList<Choice<?>>();

        expectedChoices.add(expectedChoice);
        expectedChoices.add(expectedChoice2);
        actualChoices.add(actualChoice);
        actualChoices.add(actualChoice2);

        when(expectedChoice.getValue()).thenReturn(Arrays.asList("red"));
        when(expectedChoice2.getValue()).thenReturn(Arrays.asList("blue", "black"));
        when(actualChoice.getValue()).thenReturn(Arrays.asList("red"));
        when(actualChoice2.getValue()).thenReturn(Arrays.asList("blue", "black"));

        CheckTypeUtil.compareChoices(expectedChoices, actualChoices, "type", "property");
        assertFalse(CheckTypeUtil.problems.hasErrors());

        when(actualChoice2.getDisplayName()).thenReturn("new2");
        CheckTypeUtil.compareChoices(expectedChoices, actualChoices, "type", "property");
        assertTrue(CheckTypeUtil.problems.errorCount() == 1);

        when(actualChoice2.getDisplayName()).thenReturn("new");
        when(actualChoice.getValue()).thenReturn(Arrays.asList("red2"));
        CheckTypeUtil.compareChoices(expectedChoices, actualChoices, "type", "property");
        assertTrue(CheckTypeUtil.problems.errorCount() == 2);

        when(actualChoice.getValue()).thenReturn(Arrays.asList("red"));
        when(actualChoice2.getValue()).thenReturn(Arrays.asList("blue"));
        CheckTypeUtil.compareChoices(expectedChoices, actualChoices, "type", "property");
        assertTrue(CheckTypeUtil.problems.errorCount() == 3);

        when(actualChoice.getValue()).thenReturn(Arrays.asList("red"));
        when(actualChoice2.getValue()).thenReturn(Arrays.asList("blue", "black3"));
        CheckTypeUtil.compareChoices(expectedChoices, actualChoices, "type", "property");
        assertTrue(CheckTypeUtil.problems.errorCount() == 4);
    }

    @Test
    public void shouldCompareForErrorPropertyFieldsTest() {
        CheckTypeUtil.problems = new SimpleProblems();

        CheckTypeUtil.compareForErrorPropertyFields("name", "name", "typeId", "propertyId", TypeDefinitionsIds.LOCAL_NAME);
        assertFalse(CheckTypeUtil.problems.hasErrors());

        CheckTypeUtil.compareForErrorPropertyFields(null, null, "typeId", "propertyId", TypeDefinitionsIds.LOCAL_NAME);
        assertFalse(CheckTypeUtil.problems.hasErrors());

        CheckTypeUtil.compareForErrorPropertyFields(true, true, "typeId", "propertyId", TypeDefinitionsIds.LOCAL_NAME);
        assertFalse(CheckTypeUtil.problems.hasErrors());

        CheckTypeUtil.compareForErrorPropertyFields("name", "name2", "typeId", "propertyId", TypeDefinitionsIds.LOCAL_NAME);
        assertTrue(CheckTypeUtil.problems.errorCount() == 1);

        CheckTypeUtil.compareForErrorPropertyFields(null, "name", "typeId", "propertyId", TypeDefinitionsIds.LOCAL_NAME);
        assertTrue(CheckTypeUtil.problems.errorCount() == 2);

        CheckTypeUtil.compareForErrorPropertyFields("name", null, "typeId", "propertyId", TypeDefinitionsIds.LOCAL_NAME);
        assertTrue(CheckTypeUtil.problems.errorCount() == 3);
    }

    @Test
    public void shouldCompareForWarningPropertyFieldsTest() {
        CheckTypeUtil.problems = new SimpleProblems();

        CheckTypeUtil.compareForWarningPropertyFields("name", "name", "typeId", "propertyId", TypeDefinitionsIds.LOCAL_NAME);
        assertFalse(CheckTypeUtil.problems.hasWarnings());

        CheckTypeUtil.compareForWarningPropertyFields(null, null, "typeId", "propertyId", TypeDefinitionsIds.LOCAL_NAME);
        assertFalse(CheckTypeUtil.problems.hasWarnings());

        CheckTypeUtil.compareForWarningPropertyFields(true, true, "typeId", "propertyId", TypeDefinitionsIds.LOCAL_NAME);
        assertFalse(CheckTypeUtil.problems.hasWarnings());

        CheckTypeUtil.compareForWarningPropertyFields("name", "name2", "typeId", "propertyId", TypeDefinitionsIds.LOCAL_NAME);
        assertTrue(CheckTypeUtil.problems.warningCount() == 1);

        CheckTypeUtil.compareForWarningPropertyFields(null, "name", "typeId", "propertyId", TypeDefinitionsIds.LOCAL_NAME);
        assertTrue(CheckTypeUtil.problems.warningCount() == 2);

        CheckTypeUtil.compareForWarningPropertyFields("name", null, "typeId", "propertyId", TypeDefinitionsIds.LOCAL_NAME);
        assertTrue(CheckTypeUtil.problems.warningCount() == 3);
    }

    @Test
    public void shouldCompareObjectForErrorTypeFieldsTest() {
        CheckTypeUtil.problems = new SimpleProblems();

        CheckTypeUtil.compareForErrorObjectTypeFields("name", "name", "typeId", TypeDefinitionsIds.LOCAL_NAME);
        assertFalse(CheckTypeUtil.problems.hasErrors());

        CheckTypeUtil.compareForErrorObjectTypeFields(null, null, "typeId", TypeDefinitionsIds.LOCAL_NAME);
        assertFalse(CheckTypeUtil.problems.hasErrors());

        CheckTypeUtil.compareForErrorObjectTypeFields(true, true, "typeId", TypeDefinitionsIds.LOCAL_NAME);
        assertFalse(CheckTypeUtil.problems.hasErrors());

        CheckTypeUtil.compareForErrorObjectTypeFields("name", "name2", "typeId", TypeDefinitionsIds.LOCAL_NAME);
        assertTrue(CheckTypeUtil.problems.errorCount() == 1);

        CheckTypeUtil.compareForErrorObjectTypeFields(null, "name", "typeId", TypeDefinitionsIds.LOCAL_NAME);
        assertTrue(CheckTypeUtil.problems.errorCount() == 2);

        CheckTypeUtil.compareForErrorObjectTypeFields("name", null, "typeId", TypeDefinitionsIds.LOCAL_NAME);
        assertTrue(CheckTypeUtil.problems.errorCount() == 3);
    }

    @Test
    public void shouldCompareObjectForWarningTypeFieldsTest() {
        CheckTypeUtil.problems = new SimpleProblems();

        CheckTypeUtil.compareForWarningObjectTypeFields("name", "name", "typeId", TypeDefinitionsIds.LOCAL_NAME);
        assertFalse(CheckTypeUtil.problems.hasWarnings());

        CheckTypeUtil.compareForWarningObjectTypeFields(null, null, "typeId", TypeDefinitionsIds.LOCAL_NAME);
        assertFalse(CheckTypeUtil.problems.hasWarnings());

        CheckTypeUtil.compareForWarningObjectTypeFields(true, true, "typeId", TypeDefinitionsIds.LOCAL_NAME);
        assertFalse(CheckTypeUtil.problems.hasWarnings());

        CheckTypeUtil.compareForWarningObjectTypeFields("name", "name2", "typeId", TypeDefinitionsIds.LOCAL_NAME);
        assertTrue(CheckTypeUtil.problems.warningCount() == 1);

        CheckTypeUtil.compareForWarningObjectTypeFields(null, "name", "typeId", TypeDefinitionsIds.LOCAL_NAME);
        assertTrue(CheckTypeUtil.problems.warningCount() == 2);

        CheckTypeUtil.compareForWarningObjectTypeFields("name", null, "typeId", TypeDefinitionsIds.LOCAL_NAME);
        assertTrue(CheckTypeUtil.problems.warningCount() == 3);
    }

    @Test
    public void shouldCheckPropertyTest() {
        CheckTypeUtil.problems = new SimpleProblems();

        PropertyDefinition actualProperty = mock(PropertyDefinition.class);
        PropertyDefinition expectedProperty = mock(PropertyDefinition.class);

        when(actualProperty.getDisplayName()).thenReturn("name");
        when(expectedProperty.getDisplayName()).thenReturn("name");

        when(actualProperty.getCardinality()).thenReturn(Cardinality.MULTI);
        when(expectedProperty.getCardinality()).thenReturn(Cardinality.MULTI);

        when(actualProperty.getPropertyType()).thenReturn(PropertyType.INTEGER);
        when(expectedProperty.getPropertyType()).thenReturn(PropertyType.INTEGER);

        when(actualProperty.getUpdatability()).thenReturn(Updatability.READWRITE);
        when(expectedProperty.getUpdatability()).thenReturn(Updatability.READWRITE);

        when(actualProperty.getDescription()).thenReturn("description");
        when(expectedProperty.getDescription()).thenReturn("description");

        when(actualProperty.getDefaultValue()).thenReturn(null);
        when(expectedProperty.getDefaultValue()).thenReturn(null);

        Choice actualChoice = mock(Choice.class);
        Choice expectedChoice = mock(Choice.class);
        when(expectedChoice.getValue()).thenReturn(Arrays.asList("red"));
        when(actualChoice.getValue()).thenReturn(Arrays.asList("red"));
        when(actualProperty.getChoices()).thenReturn(Arrays.asList(actualChoice));
        when(expectedProperty.getChoices()).thenReturn(Arrays.asList(expectedChoice));

        CheckTypeUtil.compareProperty(actualProperty, expectedProperty, "typeId");
        assertFalse(CheckTypeUtil.problems.hasErrors());
        assertFalse(CheckTypeUtil.problems.hasWarnings());

        when(actualProperty.getDefaultValue()).thenReturn(null);
        when(expectedProperty.getDefaultValue()).thenReturn(Arrays.asList("def"));
        CheckTypeUtil.compareProperty(actualProperty, expectedProperty, "typeId");
        assertFalse(CheckTypeUtil.problems.hasErrors());
        assertTrue(CheckTypeUtil.problems.warningCount() == 1);

        when(actualProperty.getDefaultValue()).thenReturn(Arrays.asList("def"));
        when(expectedProperty.getDefaultValue()).thenReturn(null);
        CheckTypeUtil.compareProperty(actualProperty, expectedProperty, "typeId");
        assertFalse(CheckTypeUtil.problems.hasErrors());
        assertTrue(CheckTypeUtil.problems.warningCount() == 2);

        when(expectedProperty.getDisplayName()).thenReturn("name2");
        when(expectedProperty.getCardinality()).thenReturn(Cardinality.SINGLE);
        when(expectedProperty.getPropertyType()).thenReturn(PropertyType.BOOLEAN);
        when(expectedProperty.getUpdatability()).thenReturn(Updatability.READONLY);
        when(expectedProperty.getDescription()).thenReturn("description2");
        when(actualProperty.getDefaultValue()).thenReturn(Arrays.asList("def", "def2"));

        when(actualChoice.getValue()).thenReturn(Arrays.asList("blue"));
        CheckTypeUtil.compareProperty(actualProperty, expectedProperty, "typeId");
        assertTrue(CheckTypeUtil.problems.errorCount() == 5);
        assertTrue(CheckTypeUtil.problems.warningCount() == 4);
    }

    @Test
    public void shouldCheckPropertyDefinitionsTest() {
        CheckTypeUtil.problems = new SimpleProblems();

        Map<String, PropertyDefinition<?>> expectedDefMap = new HashMap<String, PropertyDefinition<?>>();
        Map<String, PropertyDefinition<?>> actualDefMap = new HashMap<String, PropertyDefinition<?>>();

        CheckTypeUtil.comparePropertyDefinitions(expectedDefMap, actualDefMap, "typeId");
        assertFalse(CheckTypeUtil.problems.hasErrors());

        PropertyDefinition<?> expectedDef = mock(PropertyDefinition.class);
        PropertyDefinition<?> actualDef = mock(PropertyDefinition.class);
        expectedDefMap.put("prop", expectedDef);
        CheckTypeUtil.comparePropertyDefinitions(expectedDefMap, actualDefMap, "typeId");
        assertTrue(CheckTypeUtil.problems.errorCount() == 1);

        actualDefMap.put("prop", actualDef);
        CheckTypeUtil.comparePropertyDefinitions(expectedDefMap, actualDefMap, "typeId");
        assertTrue(CheckTypeUtil.problems.errorCount() == 1);

        actualDefMap.remove("prop");
        CheckTypeUtil.comparePropertyDefinitions(expectedDefMap, actualDefMap, "typeId");
        assertTrue(CheckTypeUtil.problems.errorCount() == 2);
        assertFalse(CheckTypeUtil.problems.hasWarnings());

        actualDefMap.put("prop2", actualDef);
        actualDefMap.put("prop", actualDef);
        CheckTypeUtil.comparePropertyDefinitions(expectedDefMap, actualDefMap, "typeId");
        assertTrue(CheckTypeUtil.problems.errorCount() == 2);
        assertTrue(CheckTypeUtil.problems.warningCount() == 1);

        CheckTypeUtil.comparePropertyDefinitions(null, null, "typeId");
        assertTrue(CheckTypeUtil.problems.errorCount() == 3);
    }

    @Test
    public void shouldCheckObjectTypeTest(){
        CheckTypeUtil.problems = new SimpleProblems();

        DocumentTypeImpl expectedType = mock(DocumentTypeImpl.class);
        DocumentTypeImpl actualType = mock(DocumentTypeImpl.class);

        CheckTypeUtil.compareObjectType(expectedType, actualType);
        assertFalse(CheckTypeUtil.problems.hasErrors());

        when(expectedType.getBaseTypeId()).thenReturn(BaseTypeId.CMIS_DOCUMENT);
        when(actualType.getBaseTypeId()).thenReturn(BaseTypeId.CMIS_DOCUMENT);

        when(expectedType.getQueryName()).thenReturn("query");
        when(actualType.getQueryName()).thenReturn("query");

        when(expectedType.isCreatable()).thenReturn(true);
        when(actualType.isCreatable()).thenReturn(true);

        when(expectedType.isVersionable()).thenReturn(true);
        when(actualType.isVersionable()).thenReturn(true);

        when(expectedType.getBaseTypeId()).thenReturn(BaseTypeId.CMIS_DOCUMENT);
        when(actualType.getBaseTypeId()).thenReturn(BaseTypeId.CMIS_DOCUMENT);

        when(expectedType.getDescription()).thenReturn("description");
        when(actualType.getDescription()).thenReturn("description");

        CheckTypeUtil.compareObjectType(expectedType, actualType);
        verify(expectedType, times(2)).isVersionable();
        verify(actualType, times(2)).isVersionable();
        assertFalse(CheckTypeUtil.problems.hasErrors());
        assertFalse(CheckTypeUtil.problems.hasWarnings());

        when(actualType.getQueryName()).thenReturn("query2");
        when(actualType.isCreatable()).thenReturn(false);
        when(actualType.isVersionable()).thenReturn(false);
        when(actualType.getDescription()).thenReturn("description2");
        CheckTypeUtil.compareObjectType(expectedType, actualType);
        assertTrue(CheckTypeUtil.problems.errorCount() == 3);
        assertTrue(CheckTypeUtil.problems.warningCount() == 1);

        CheckTypeUtil.compareObjectType(null, null);
        assertTrue(CheckTypeUtil.problems.errorCount() == 4);
    }

    @Test
    public void shouldCheckTypeDefinitionsTest(){

        Problems problems;

        Map<String, ObjectType> expectedDefMap = new HashMap<String, ObjectType>();
        Map<String, ObjectType> actualDefMap = new HashMap<String, ObjectType>();

        problems = CheckTypeUtil.checkTypeDefinitions(expectedDefMap, actualDefMap);
        assertFalse(problems.hasErrors());

        ObjectType expectedDef = mock(ObjectType.class);
        ObjectType actualDef = mock(ObjectType.class);
        when(expectedDef.isBaseType()).thenReturn(true);
        expectedDefMap.put("type", expectedDef);
        problems = CheckTypeUtil.checkTypeDefinitions(expectedDefMap, actualDefMap);
        assertTrue(problems.errorCount() == 1);
        assertFalse(problems.hasWarnings());

        actualDefMap.put("type", actualDef);
        problems = CheckTypeUtil.checkTypeDefinitions(expectedDefMap, actualDefMap);
        assertTrue(problems.errorCount() == 0);
        assertFalse(problems.hasWarnings());

        actualDefMap.remove("type");
        problems = CheckTypeUtil.checkTypeDefinitions(expectedDefMap, actualDefMap);
        assertTrue(problems.errorCount() == 1);
        assertFalse(problems.hasWarnings());
        verify(expectedDef, times(0)).getDisplayName();
        verify(actualDef, times(0)).getDisplayName();

        when(expectedDef.isBaseType()).thenReturn(false);
        actualDefMap.put("type2", actualDef);
        actualDefMap.put("type", actualDef);
        problems = CheckTypeUtil.checkTypeDefinitions(expectedDefMap, actualDefMap);
        assertTrue(problems.warningCount() == 1);
        assertFalse(problems.hasErrors());
        verify(expectedDef, times(1)).getDisplayName();
        verify(actualDef, times(1)).getDisplayName();

        problems = CheckTypeUtil.checkTypeDefinitions(null, null);
        assertTrue(problems.errorCount() == 1);
        assertFalse(problems.hasWarnings());
    }
}
