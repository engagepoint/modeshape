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
import org.modeshape.connector.cmis.common.CompareTypesI18n;
import org.modeshape.connector.cmis.common.TypeDefinitionsIds;

import java.util.*;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.*;

/**
 * JUnit Test for {@link CompareTypeDefinitionsUtil}
 * Created by vyacheslav.polulyakh on 5/15/2014.
 */
public class CompareTypeDefinitionsUtilTest {

    @Test
    public void shouldCompareNullsValuesTest() {

        assertTrue(CompareTypeDefinitionsUtil.compareValues("string", "string"));
        assertTrue(CompareTypeDefinitionsUtil.compareValues(null, null));
        assertFalse(CompareTypeDefinitionsUtil.compareValues(null, "string"));
        assertFalse(CompareTypeDefinitionsUtil.compareValues("string", null));
    }

    @Test
    public void shouldVerifyIsNullValuesTest() {

        CompareTypeDefinitionsUtil.problems = new SimpleProblems();

        assertTrue(CompareTypeDefinitionsUtil.isNullValues(null, null));
        assertTrue(CompareTypeDefinitionsUtil.problems.errorCount() == 1);

        assertTrue(CompareTypeDefinitionsUtil.isNullValues("string", "string", null));
        assertTrue(CompareTypeDefinitionsUtil.problems.errorCount() == 2);

        assertFalse(CompareTypeDefinitionsUtil.isNullValues("string", "string"));
        assertTrue(CompareTypeDefinitionsUtil.problems.errorCount() == 2);
    }

    @Test
    public void shouldCompareMapsTest() {
        CompareTypeDefinitionsUtil.problems = new SimpleProblems();
        Map<String, String> expectedMap = new HashMap<String, String>();
        Map<String, String> actualMap = new HashMap<String, String>();

        expectedMap.put("key1", "string");
        actualMap.put("key1", "string");
        CompareTypeDefinitionsUtil.compareMaps(expectedMap, actualMap, CompareTypesI18n.typeWas, null);
        assertFalse(CompareTypeDefinitionsUtil.problems.hasErrors());
        assertTrue(expectedMap.size() == 1);

        expectedMap.put("key2", "string2");
        expectedMap = CompareTypeDefinitionsUtil.compareMaps(expectedMap, actualMap, CompareTypesI18n.typeWas, null);
        assertTrue(CompareTypeDefinitionsUtil.problems.errorCount() == 1);
        assertTrue(CompareTypeDefinitionsUtil.problems.warningCount() == 0);
        assertTrue(expectedMap.size() == 1);
        assertTrue(CompareTypeDefinitionsUtil.problems.iterator().next().getMessage().equals(CompareTypesI18n.typeWas));

        actualMap.put("key2", "string2");
        expectedMap = CompareTypeDefinitionsUtil.compareMaps(expectedMap, actualMap, CompareTypesI18n.propertyWas, "type1");
        assertTrue(CompareTypeDefinitionsUtil.problems.errorCount() == 1);
        assertTrue(CompareTypeDefinitionsUtil.problems.warningCount() == 1);
        assertTrue(expectedMap.size() == 1);
    }

    @Test
    public void shouldCheckChoice() {
        CompareTypeDefinitionsUtil.problems = new SimpleProblems();
        Choice expectedChoice = mock(Choice.class);
        Choice actualChoice = mock(Choice.class);

        when(expectedChoice.getDisplayName()).thenReturn("");
        when(actualChoice.getDisplayName()).thenReturn("");
        when(expectedChoice.getValue()).thenReturn(Collections.emptyList());
        when(actualChoice.getValue()).thenReturn(Collections.emptyList());
        CompareTypeDefinitionsUtil.compareChoice(expectedChoice, actualChoice, "type", "property");
        assertFalse(CompareTypeDefinitionsUtil.problems.hasErrors());

        when(expectedChoice.getValue()).thenReturn(Arrays.asList("blue"));
        when(actualChoice.getValue()).thenReturn(Arrays.asList("blue"));
        CompareTypeDefinitionsUtil.compareChoice(expectedChoice, actualChoice, "type", "property");
        assertFalse(CompareTypeDefinitionsUtil.problems.hasErrors());

        when(expectedChoice.getValue()).thenReturn(Arrays.asList("blue", "red"));
        when(actualChoice.getValue()).thenReturn(Arrays.asList("blue", "red"));
        CompareTypeDefinitionsUtil.compareChoice(expectedChoice, actualChoice, "type", "property");
        assertFalse(CompareTypeDefinitionsUtil.problems.hasErrors());

        when(expectedChoice.getValue()).thenReturn(Arrays.asList("blue", "red"));
        when(actualChoice.getValue()).thenReturn(Arrays.asList("blue"));
        CompareTypeDefinitionsUtil.compareChoice(expectedChoice, actualChoice, "type", "property");
        assertTrue(CompareTypeDefinitionsUtil.problems.errorCount() == 1);

        when(expectedChoice.getValue()).thenReturn(Arrays.asList("blue", "red"));
        when(actualChoice.getValue()).thenReturn(Arrays.asList("blue", "red2"));
        CompareTypeDefinitionsUtil.compareChoice(expectedChoice, actualChoice, "type", "property");
        assertTrue(CompareTypeDefinitionsUtil.problems.errorCount() == 2);

        when(expectedChoice.getValue()).thenReturn(Arrays.asList(5, 7));
        when(actualChoice.getValue()).thenReturn(Arrays.asList(5, 7));
        CompareTypeDefinitionsUtil.compareChoice(expectedChoice, actualChoice, "type", "property");
        assertTrue(CompareTypeDefinitionsUtil.problems.errorCount() == 2);

        when(expectedChoice.getValue()).thenReturn(Arrays.asList("blue", "red"));
        when(actualChoice.getValue()).thenReturn(Collections.emptyList());
        CompareTypeDefinitionsUtil.compareChoice(expectedChoice, actualChoice, "type", "property");
        assertTrue(CompareTypeDefinitionsUtil.problems.errorCount() == 3);
    }

    @Test
    public void shouldCheckChoicesTest() {
        CompareTypeDefinitionsUtil.problems = new SimpleProblems();
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

        CompareTypeDefinitionsUtil.compareChoices(expectedChoices, actualChoices, "type", "property");
        assertFalse(CompareTypeDefinitionsUtil.problems.hasErrors());

        when(actualChoice2.getDisplayName()).thenReturn("new2");
        CompareTypeDefinitionsUtil.compareChoices(expectedChoices, actualChoices, "type", "property");
        assertTrue(CompareTypeDefinitionsUtil.problems.errorCount() == 1);

        when(actualChoice2.getDisplayName()).thenReturn("new");
        when(actualChoice.getValue()).thenReturn(Arrays.asList("red2"));
        CompareTypeDefinitionsUtil.compareChoices(expectedChoices, actualChoices, "type", "property");
        assertTrue(CompareTypeDefinitionsUtil.problems.errorCount() == 2);

        when(actualChoice.getValue()).thenReturn(Arrays.asList("red"));
        when(actualChoice2.getValue()).thenReturn(Arrays.asList("blue"));
        CompareTypeDefinitionsUtil.compareChoices(expectedChoices, actualChoices, "type", "property");
        assertTrue(CompareTypeDefinitionsUtil.problems.errorCount() == 3);

        when(actualChoice.getValue()).thenReturn(Arrays.asList("red"));
        when(actualChoice2.getValue()).thenReturn(Arrays.asList("blue", "black3"));
        CompareTypeDefinitionsUtil.compareChoices(expectedChoices, actualChoices, "type", "property");
        assertTrue(CompareTypeDefinitionsUtil.problems.errorCount() == 4);
    }

    @Test
    public void shouldCompareForErrorPropertyFieldsTest() {
        CompareTypeDefinitionsUtil.problems = new SimpleProblems();

        CompareTypeDefinitionsUtil.compareForErrorPropertyFields("name", "name", "typeId", "propertyId", TypeDefinitionsIds.LOCAL_NAME);
        assertFalse(CompareTypeDefinitionsUtil.problems.hasErrors());

        CompareTypeDefinitionsUtil.compareForErrorPropertyFields(null, null, "typeId", "propertyId", TypeDefinitionsIds.LOCAL_NAME);
        assertFalse(CompareTypeDefinitionsUtil.problems.hasErrors());

        CompareTypeDefinitionsUtil.compareForErrorPropertyFields(true, true, "typeId", "propertyId", TypeDefinitionsIds.LOCAL_NAME);
        assertFalse(CompareTypeDefinitionsUtil.problems.hasErrors());

        CompareTypeDefinitionsUtil.compareForErrorPropertyFields("name", "name2", "typeId", "propertyId", TypeDefinitionsIds.LOCAL_NAME);
        assertTrue(CompareTypeDefinitionsUtil.problems.errorCount() == 1);

        CompareTypeDefinitionsUtil.compareForErrorPropertyFields(null, "name", "typeId", "propertyId", TypeDefinitionsIds.LOCAL_NAME);
        assertTrue(CompareTypeDefinitionsUtil.problems.errorCount() == 2);

        CompareTypeDefinitionsUtil.compareForErrorPropertyFields("name", null, "typeId", "propertyId", TypeDefinitionsIds.LOCAL_NAME);
        assertTrue(CompareTypeDefinitionsUtil.problems.errorCount() == 3);
    }

    @Test
    public void shouldCompareForWarningPropertyFieldsTest() {
        CompareTypeDefinitionsUtil.problems = new SimpleProblems();

        CompareTypeDefinitionsUtil.compareForWarningPropertyFields("name", "name", "typeId", "propertyId", TypeDefinitionsIds.LOCAL_NAME);
        assertFalse(CompareTypeDefinitionsUtil.problems.hasWarnings());

        CompareTypeDefinitionsUtil.compareForWarningPropertyFields(null, null, "typeId", "propertyId", TypeDefinitionsIds.LOCAL_NAME);
        assertFalse(CompareTypeDefinitionsUtil.problems.hasWarnings());

        CompareTypeDefinitionsUtil.compareForWarningPropertyFields(true, true, "typeId", "propertyId", TypeDefinitionsIds.LOCAL_NAME);
        assertFalse(CompareTypeDefinitionsUtil.problems.hasWarnings());

        CompareTypeDefinitionsUtil.compareForWarningPropertyFields("name", "name2", "typeId", "propertyId", TypeDefinitionsIds.LOCAL_NAME);
        assertTrue(CompareTypeDefinitionsUtil.problems.warningCount() == 1);

        CompareTypeDefinitionsUtil.compareForWarningPropertyFields(null, "name", "typeId", "propertyId", TypeDefinitionsIds.LOCAL_NAME);
        assertTrue(CompareTypeDefinitionsUtil.problems.warningCount() == 2);

        CompareTypeDefinitionsUtil.compareForWarningPropertyFields("name", null, "typeId", "propertyId", TypeDefinitionsIds.LOCAL_NAME);
        assertTrue(CompareTypeDefinitionsUtil.problems.warningCount() == 3);
    }

    @Test
    public void shouldCompareObjectForErrorTypeFieldsTest() {
        CompareTypeDefinitionsUtil.problems = new SimpleProblems();

        CompareTypeDefinitionsUtil.compareForErrorObjectTypeFields("name", "name", "typeId", TypeDefinitionsIds.LOCAL_NAME);
        assertFalse(CompareTypeDefinitionsUtil.problems.hasErrors());

        CompareTypeDefinitionsUtil.compareForErrorObjectTypeFields(null, null, "typeId", TypeDefinitionsIds.LOCAL_NAME);
        assertFalse(CompareTypeDefinitionsUtil.problems.hasErrors());

        CompareTypeDefinitionsUtil.compareForErrorObjectTypeFields(true, true, "typeId", TypeDefinitionsIds.LOCAL_NAME);
        assertFalse(CompareTypeDefinitionsUtil.problems.hasErrors());

        CompareTypeDefinitionsUtil.compareForErrorObjectTypeFields("name", "name2", "typeId", TypeDefinitionsIds.LOCAL_NAME);
        assertTrue(CompareTypeDefinitionsUtil.problems.errorCount() == 1);

        CompareTypeDefinitionsUtil.compareForErrorObjectTypeFields(null, "name", "typeId", TypeDefinitionsIds.LOCAL_NAME);
        assertTrue(CompareTypeDefinitionsUtil.problems.errorCount() == 2);

        CompareTypeDefinitionsUtil.compareForErrorObjectTypeFields("name", null, "typeId", TypeDefinitionsIds.LOCAL_NAME);
        assertTrue(CompareTypeDefinitionsUtil.problems.errorCount() == 3);
    }

    @Test
    public void shouldCompareObjectForWarningTypeFieldsTest() {
        CompareTypeDefinitionsUtil.problems = new SimpleProblems();

        CompareTypeDefinitionsUtil.compareForWarningObjectTypeFields("name", "name", "typeId", TypeDefinitionsIds.LOCAL_NAME);
        assertFalse(CompareTypeDefinitionsUtil.problems.hasWarnings());

        CompareTypeDefinitionsUtil.compareForWarningObjectTypeFields(null, null, "typeId", TypeDefinitionsIds.LOCAL_NAME);
        assertFalse(CompareTypeDefinitionsUtil.problems.hasWarnings());

        CompareTypeDefinitionsUtil.compareForWarningObjectTypeFields(true, true, "typeId", TypeDefinitionsIds.LOCAL_NAME);
        assertFalse(CompareTypeDefinitionsUtil.problems.hasWarnings());

        CompareTypeDefinitionsUtil.compareForWarningObjectTypeFields("name", "name2", "typeId", TypeDefinitionsIds.LOCAL_NAME);
        assertTrue(CompareTypeDefinitionsUtil.problems.warningCount() == 1);

        CompareTypeDefinitionsUtil.compareForWarningObjectTypeFields(null, "name", "typeId", TypeDefinitionsIds.LOCAL_NAME);
        assertTrue(CompareTypeDefinitionsUtil.problems.warningCount() == 2);

        CompareTypeDefinitionsUtil.compareForWarningObjectTypeFields("name", null, "typeId", TypeDefinitionsIds.LOCAL_NAME);
        assertTrue(CompareTypeDefinitionsUtil.problems.warningCount() == 3);
    }

    @Test
    public void shouldCheckPropertyTest() {
        CompareTypeDefinitionsUtil.problems = new SimpleProblems();

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

        CompareTypeDefinitionsUtil.compareProperty(actualProperty, expectedProperty, "typeId");
        assertFalse(CompareTypeDefinitionsUtil.problems.hasErrors());
        assertFalse(CompareTypeDefinitionsUtil.problems.hasWarnings());

        when(actualProperty.getDefaultValue()).thenReturn(null);
        when(expectedProperty.getDefaultValue()).thenReturn(Arrays.asList("def"));
        CompareTypeDefinitionsUtil.compareProperty(actualProperty, expectedProperty, "typeId");
        assertFalse(CompareTypeDefinitionsUtil.problems.hasErrors());
        assertTrue(CompareTypeDefinitionsUtil.problems.warningCount() == 1);

        when(actualProperty.getDefaultValue()).thenReturn(Arrays.asList("def"));
        when(expectedProperty.getDefaultValue()).thenReturn(null);
        CompareTypeDefinitionsUtil.compareProperty(actualProperty, expectedProperty, "typeId");
        assertFalse(CompareTypeDefinitionsUtil.problems.hasErrors());
        assertTrue(CompareTypeDefinitionsUtil.problems.warningCount() == 2);

        when(expectedProperty.getDisplayName()).thenReturn("name2");
        when(expectedProperty.getCardinality()).thenReturn(Cardinality.SINGLE);
        when(expectedProperty.getPropertyType()).thenReturn(PropertyType.BOOLEAN);
        when(expectedProperty.getUpdatability()).thenReturn(Updatability.READONLY);
        when(expectedProperty.getDescription()).thenReturn("description2");
        when(actualProperty.getDefaultValue()).thenReturn(Arrays.asList("def", "def2"));

        when(actualChoice.getValue()).thenReturn(Arrays.asList("blue"));
        CompareTypeDefinitionsUtil.compareProperty(actualProperty, expectedProperty, "typeId");
        assertTrue(CompareTypeDefinitionsUtil.problems.errorCount() == 5);
        assertTrue(CompareTypeDefinitionsUtil.problems.warningCount() == 4);
    }

    @Test
    public void shouldCheckPropertyDefinitionsTest() {
        CompareTypeDefinitionsUtil.problems = new SimpleProblems();

        Map<String, PropertyDefinition<?>> expectedDefMap = new HashMap<String, PropertyDefinition<?>>();
        Map<String, PropertyDefinition<?>> actualDefMap = new HashMap<String, PropertyDefinition<?>>();

        CompareTypeDefinitionsUtil.comparePropertyDefinitions(expectedDefMap, actualDefMap, "typeId");
        assertFalse(CompareTypeDefinitionsUtil.problems.hasErrors());

        PropertyDefinition<?> expectedDef = mock(PropertyDefinition.class);
        PropertyDefinition<?> actualDef = mock(PropertyDefinition.class);
        expectedDefMap.put("prop", expectedDef);
        CompareTypeDefinitionsUtil.comparePropertyDefinitions(expectedDefMap, actualDefMap, "typeId");
        assertTrue(CompareTypeDefinitionsUtil.problems.errorCount() == 1);

        actualDefMap.put("prop", actualDef);
        CompareTypeDefinitionsUtil.comparePropertyDefinitions(expectedDefMap, actualDefMap, "typeId");
        assertTrue(CompareTypeDefinitionsUtil.problems.errorCount() == 1);

        actualDefMap.remove("prop");
        CompareTypeDefinitionsUtil.comparePropertyDefinitions(expectedDefMap, actualDefMap, "typeId");
        assertTrue(CompareTypeDefinitionsUtil.problems.errorCount() == 2);
        assertFalse(CompareTypeDefinitionsUtil.problems.hasWarnings());

        actualDefMap.put("prop2", actualDef);
        actualDefMap.put("prop", actualDef);
        CompareTypeDefinitionsUtil.comparePropertyDefinitions(expectedDefMap, actualDefMap, "typeId");
        assertTrue(CompareTypeDefinitionsUtil.problems.errorCount() == 2);
        assertTrue(CompareTypeDefinitionsUtil.problems.warningCount() == 1);

        CompareTypeDefinitionsUtil.comparePropertyDefinitions(null, null, "typeId");
        assertTrue(CompareTypeDefinitionsUtil.problems.errorCount() == 3);
    }

    @Test
    public void shouldCheckObjectTypeTest(){
        CompareTypeDefinitionsUtil.problems = new SimpleProblems();

        DocumentTypeImpl expectedType = mock(DocumentTypeImpl.class);
        DocumentTypeImpl actualType = mock(DocumentTypeImpl.class);

        CompareTypeDefinitionsUtil.compareObjectType(expectedType, actualType);
        assertFalse(CompareTypeDefinitionsUtil.problems.hasErrors());

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

        CompareTypeDefinitionsUtil.compareObjectType(expectedType, actualType);
        verify(expectedType, times(2)).isVersionable();
        verify(actualType, times(2)).isVersionable();
        assertFalse(CompareTypeDefinitionsUtil.problems.hasErrors());
        assertFalse(CompareTypeDefinitionsUtil.problems.hasWarnings());

        when(actualType.getQueryName()).thenReturn("query2");
        when(actualType.isCreatable()).thenReturn(false);
        when(actualType.isVersionable()).thenReturn(false);
        when(actualType.getDescription()).thenReturn("description2");
        CompareTypeDefinitionsUtil.compareObjectType(expectedType, actualType);
        assertTrue(CompareTypeDefinitionsUtil.problems.errorCount() == 3);
        assertTrue(CompareTypeDefinitionsUtil.problems.warningCount() == 1);

        CompareTypeDefinitionsUtil.compareObjectType(null, null);
        assertTrue(CompareTypeDefinitionsUtil.problems.errorCount() == 4);
    }

    @Test
    public void shouldCheckTypeDefinitionsTest(){

        Problems problems;

        Map<String, ObjectType> expectedDefMap = new HashMap<String, ObjectType>();
        Map<String, ObjectType> actualDefMap = new HashMap<String, ObjectType>();

        problems = CompareTypeDefinitionsUtil.compareTypeDefinitions(expectedDefMap, actualDefMap);
        assertFalse(problems.hasErrors());

        ObjectType expectedDef = mock(ObjectType.class);
        ObjectType actualDef = mock(ObjectType.class);
        when(expectedDef.isBaseType()).thenReturn(true);
        expectedDefMap.put("type", expectedDef);
        problems = CompareTypeDefinitionsUtil.compareTypeDefinitions(expectedDefMap, actualDefMap);
        assertTrue(problems.errorCount() == 1);
        assertFalse(problems.hasWarnings());

        actualDefMap.put("type", actualDef);
        problems = CompareTypeDefinitionsUtil.compareTypeDefinitions(expectedDefMap, actualDefMap);
        assertTrue(problems.errorCount() == 0);
        assertFalse(problems.hasWarnings());

        actualDefMap.remove("type");
        problems = CompareTypeDefinitionsUtil.compareTypeDefinitions(expectedDefMap, actualDefMap);
        assertTrue(problems.errorCount() == 1);
        assertFalse(problems.hasWarnings());
        verify(expectedDef, times(0)).getDisplayName();
        verify(actualDef, times(0)).getDisplayName();

        when(expectedDef.isBaseType()).thenReturn(false);
        actualDefMap.put("type2", actualDef);
        actualDefMap.put("type", actualDef);
        problems = CompareTypeDefinitionsUtil.compareTypeDefinitions(expectedDefMap, actualDefMap);
        assertTrue(problems.warningCount() == 1);
        assertFalse(problems.hasErrors());
        verify(expectedDef, times(1)).getDisplayName();
        verify(actualDef, times(1)).getDisplayName();

        problems = CompareTypeDefinitionsUtil.compareTypeDefinitions(null, null);
        assertTrue(problems.errorCount() == 1);
        assertFalse(problems.hasWarnings());
    }
}
