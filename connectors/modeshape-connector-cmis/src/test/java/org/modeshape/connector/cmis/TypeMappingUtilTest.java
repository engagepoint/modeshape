package org.modeshape.connector.cmis;


import junit.framework.Assert;
import org.infinispan.schematic.document.ParsingException;
import org.junit.Test;
import org.modeshape.common.collection.SimpleProblems;
import org.modeshape.connector.cmis.config.TypeCustomMappingList;
import org.modeshape.connector.cmis.mapping.MappedCustomType;
import org.modeshape.connector.cmis.mapping.MappedTypesContainer;
import org.modeshape.connector.cmis.util.TypeMappingConfigUtil;
import org.modeshape.jcr.RepositoryConfiguration;

import java.io.FileNotFoundException;
import java.io.InputStream;
import java.util.Date;
import java.util.List;

public class TypeMappingUtilTest {

    public static final String REPO_CONFIG_FILE = "config/repository-1.json";

    @Test
    public void shouldLoadMappedTypeStep1() throws Exception {
        ClassLoader classLoader = this.getClass().getClassLoader();
        InputStream resourceAsStream = classLoader.getResourceAsStream(REPO_CONFIG_FILE);
        RepositoryConfiguration config = RepositoryConfiguration.read(resourceAsStream, "repo");

        List<RepositoryConfiguration.Component> connectors = config.getFederation().getConnectors(new SimpleProblems());
        CmisConnector cmisConnector = connectors.get(0).createInstance(classLoader);
        TypeCustomMappingList customMapping = cmisConnector.customMapping;

        MappedTypesContainer mappedTypes = TypeMappingConfigUtil.getMappedTypes(customMapping, null);

        MappedCustomType mappedTypeTwo = mappedTypes.findByJcrName("custom:singleVersionTypeTwoMapped");
        Assert.assertEquals("custom:singleVersionTypeTwo", mappedTypeTwo.getExtName());
        Assert.assertEquals("custom:artistTwo", mappedTypeTwo.toExtProperty("custom:artistTwoMapped"));
    }

}
