package org.modeshape.connector.cmis.features;

import org.apache.commons.lang3.StringUtils;
import org.infinispan.schematic.document.Document;
import org.modeshape.connector.cmis.api.SecondaryIdProcessor;
import org.modeshape.connector.cmis.mapping.LocalTypeManager;
import org.modeshape.jcr.RepositoryConfiguration;
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
    private String singleVersionGUIDPrefix = "";
    private String commonIdProcessorClass;

    //  common id as child reference
    private boolean useCommonIdAsPrimary = true;

    // runtime property
    private Set<Name> singleVersionTypeNames = new HashSet<Name>();
    private Set<String> singleVersionExternalTypeNames = new HashSet<String>();
    private SecondaryIdProcessor commonIdProcessorInstance;
    private boolean configured;

    public int initialize(ValueFactories factories, LocalTypeManager localTypeManager) throws Exception {
        if (singleVersionTypes == null) return 0;

        for (String typeName : singleVersionTypes) {
            singleVersionTypeNames.add(factories.getNameFactory().create(typeName));
            String extName = localTypeManager.getMappedTypes().findByJcrName(typeName).getExtName();
            singleVersionExternalTypeNames.add(extName);
        }

        // !?
        Class<?> aClass = this.getClass().getClassLoader().loadClass(commonIdProcessorClass);
        commonIdProcessorInstance = (SecondaryIdProcessor) aClass.newInstance();

        configured = StringUtils.isNotEmpty(commonIdPropertyName)
                    && StringUtils.isNotEmpty(commonIdTypeName)
                    && StringUtils.isNotEmpty(commonIdQuery);

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

    public Set<String> getSingleVersionExternalTypeNames() {
        return singleVersionExternalTypeNames;
    }

    public SecondaryIdProcessor getCommonIdProcessorInstance() {
        return commonIdProcessorInstance;
    }

    public String commonIdValuePreProcess(String value) {
        return (commonIdProcessorInstance == null)
                ? value
                : commonIdProcessorInstance.preProcessIdValue(value);
    }

    public boolean isConfigured() {
        return configured;
    }
}
