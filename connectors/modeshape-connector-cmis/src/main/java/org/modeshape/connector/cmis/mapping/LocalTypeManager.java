package org.modeshape.connector.cmis.mapping;

import org.apache.chemistry.opencmis.client.api.ObjectType;
import org.apache.chemistry.opencmis.client.api.Session;
import org.apache.chemistry.opencmis.client.api.Tree;
import org.apache.chemistry.opencmis.commons.definitions.Choice;
import org.apache.chemistry.opencmis.commons.definitions.PropertyDefinition;
import org.apache.chemistry.opencmis.commons.enums.BaseTypeId;
import org.apache.chemistry.opencmis.commons.enums.Cardinality;
import org.apache.chemistry.opencmis.commons.enums.Updatability;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.modeshape.connector.cmis.CmisLexicon;
import org.modeshape.connector.cmis.Constants;
import org.modeshape.connector.cmis.config.TypeCustomMappingList;
import org.modeshape.connector.cmis.util.TypeMappingConfigUtil;
import org.modeshape.jcr.JcrNodeTypeManager;
import org.modeshape.jcr.JcrNodeTypeTemplate;
import org.modeshape.jcr.api.JcrConstants;
import org.modeshape.jcr.api.nodetype.NodeTypeManager;
import org.modeshape.jcr.value.Name;
import org.modeshape.jcr.value.ValueFactories;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jcr.NamespaceRegistry;
import javax.jcr.RepositoryException;
import javax.jcr.nodetype.NoSuchNodeTypeException;
import javax.jcr.nodetype.NodeDefinitionTemplate;
import javax.jcr.nodetype.NodeType;
import javax.jcr.nodetype.NodeTypeDefinition;
import javax.jcr.nodetype.NodeTypeTemplate;
import javax.jcr.nodetype.PropertyDefinitionTemplate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;


public class LocalTypeManager {

    /**
     * SLF logger.
     */
    private final org.slf4j.Logger LOG = LoggerFactory.getLogger(this.getClass());

    private ValueFactories factories;
    private MappedTypesContainer mappedTypes;
    private Map<String, String> mappedNamespaces = new HashMap<String, String>();
    private Collection<Name> applicableTypesInstance;
    private Prefix prefixes = new Prefix();
    private org.modeshape.connector.cmis.mapping.Properties properties;
    private Nodes nodes;
    private Map<String, ObjectType> cachedTypeDefinitions = new HashMap<String, ObjectType>();
    private NodeTypeManager nodeTypeManager;
    private NamespaceRegistry registry;
    private boolean debug = true;
    private Logger log;
    /**
     * Map of all registered properties, where key is "external name"
     * and value is "jcr name".
     */
    private Map<String, String> registeredProperties;
    /**
     * Set of propertyNames(External names) which will be ignored
     * on query result processing.
     */
    private Set<String> globalIgnoredExtProperties = new HashSet<String>(10);

    public LocalTypeManager(ValueFactories factories,
                            NamespaceRegistry registry,
                            NodeTypeManager nodeTypeManager,
                            TypeCustomMappingList customMapping) {
        this.factories = factories;
        this.properties = new org.modeshape.connector.cmis.mapping.Properties(factories);
        this.nodes = new Nodes();
        this.nodeTypeManager = nodeTypeManager;
        this.registry = registry;
        this.registeredProperties = new HashMap<String, String>(100);
        this.mappedTypes = getCompleteMappings(customMapping);
        if (customMapping != null && customMapping.getNamespaces() != null)
            this.mappedNamespaces.putAll(customMapping.getNamespaces());
        if (customMapping.getGlobalIgnoredExtProperties() != null) {
            this.globalIgnoredExtProperties
                    .addAll(customMapping.getGlobalIgnoredExtProperties());
        }
    }

    public MappedTypesContainer getCompleteMappings(TypeCustomMappingList customMapping) {
        MappedCustomType defaultPropertyMapping = getDefaultPropertyMappings();
        MappedTypesContainer result = TypeMappingConfigUtil.getMappedTypes(customMapping, defaultPropertyMapping);

        // add standard file and folder mappings
        result.addTypeMapping(new MappedCustomType(NodeType.NT_FOLDER, BaseTypeId.CMIS_FOLDER.value(), defaultPropertyMapping));
        result.addTypeMapping(new MappedCustomType(NodeType.NT_FILE, BaseTypeId.CMIS_DOCUMENT.value(), defaultPropertyMapping));

        return result;
    }

    public static MappedCustomType getDefaultPropertyMappings() {
        MappedCustomType result = new MappedCustomType("DEFAULT_MAPPINGS", "DEFAULT_MAPPINGS");
        // mappings
        result.addPropertyMapping("jcr:uuid", "cmis:objectId");
        result.addPropertyMapping("jcr:createdBy", "cmis:createdBy");
        result.addPropertyMapping("jcr:created", "cmis:creationDate");
        result.addPropertyMapping("jcr:lastModified", "cmis:lastModificationDate");
        // ignores
        result.setIgnoreExternalProperties(Arrays.asList("cmis:lastModifiedBy"));

        return result;
    }

    public org.modeshape.connector.cmis.mapping.Properties getPropertyUtils() {
        return properties;
    }

    public Nodes getNodes() {
        return nodes;
    }

    public MappedTypesContainer getMappedTypes() {
        return mappedTypes;
    }

    public Collection<Name> getApplicableTypesInstance() {
        return applicableTypesInstance;
    }

    public Prefix getPrefixes() {
        return prefixes;
    }

    public Map<String, ObjectType> getCachedTypeDefinitions() {
        return cachedTypeDefinitions;
    }

    public ValueFactories getFactories() {
        return factories;
    }

    private void initializeApplicableUnfiledTypes(String[] applicableUnfiledTypes) {
        if (applicableUnfiledTypes != null && applicableUnfiledTypes.length > 0) {
            applicableTypesInstance = new HashSet<Name>(applicableUnfiledTypes.length);
            for (String unfiledType : applicableUnfiledTypes) {
                Name name = factories.getNameFactory().create(unfiledType);
                applicableTypesInstance.add(name);
            }
            applicableTypesInstance = Collections.unmodifiableCollection(applicableTypesInstance);
        } else {
            applicableTypesInstance = Collections.emptySet();
        }
    }


    public void initialize(Session session, String[] applicableUnfiledTypes) throws RepositoryException {
        // register new types from external repository
        registerPredefinedNamspaces(registry);
//        importTypes(session.getTypeDescendants(null, Integer.MAX_VALUE, true), nodeTypeManager, registry);
        List<NodeTypeTemplate> definitionsList = new ArrayList<NodeTypeTemplate>();
        importTypes(session.getTypeDescendants(null, Integer.MAX_VALUE, true), nodeTypeManager, registry, definitionsList);
        updateTypes(nodeTypeManager, definitionsList);
        // after importing and updating - we need to register/update all types
        NodeTypeDefinition[] nodeDefs = new NodeTypeDefinition[definitionsList.size()];
        definitionsList.toArray(nodeDefs);

        for (int i = 0; i < nodeDefs.length; i++) {
            String nodeTypeName = nodeDefs[i].getName();
            try {
                ((JcrNodeTypeManager) nodeTypeManager).getNodeType(nodeTypeName);
                throw new RepositoryException(String.format("There is already imported type with id %s. Try to check types in configurations ", nodeTypeName));
            } catch (NoSuchNodeTypeException e) {
                LOG.debug(String.format("registered new type: %s",nodeTypeName));
            }
        }

        nodeTypeManager.registerNodeTypes(nodeDefs, true);
        // todo: reimport, use types from manager
        registerRepositoryInfoType(nodeTypeManager);
        initializeApplicableUnfiledTypes(applicableUnfiledTypes);
    }

    // ------------------------------ TYPE MANAGEMENT -----------------------

    public ObjectType getTypeDefinition(Session session, String typeId) {
        if (!cachedTypeDefinitions.containsKey(typeId)) {
            ObjectType typeDefinition = session.getTypeDefinition(typeId);
            cachedTypeDefinitions.put(typeId, typeDefinition);
        }
        return cachedTypeDefinitions.get(typeId);
    }

    /*
    * register default CMIS namespace
    * and namespaces from mapping section of connector's configuration
    *
    * do it before node types registration because:
    * - ns uri is not necessary per type in this case
    * - problems may appear when registering namespace together with nodetype (but probably may be fixed)
   */
    public void registerPredefinedNamspaces(NamespaceRegistry registry) throws RepositoryException {
        // modeshape cmis
        registry.registerNamespace(CmisLexicon.Namespace.PREFIX, CmisLexicon.Namespace.URI);

        if (mappedNamespaces.isEmpty()) return;

        // custom
        for (Map.Entry<String, String> entry : mappedNamespaces.entrySet()) {
            String nsPrefix = entry.getKey();
            String nsUri = entry.getValue();
            if (!isNsAlreadyRegistered(null, registry, nsPrefix, nsUri)) {
                registry.registerNamespace(nsPrefix, nsUri);
            }
            prefixes.addNamespace(nsPrefix, nsUri);
        }
    }

    /**
     * Import CMIS types to JCR repository.
     *
     * @param types       CMIS types
     * @param typeManager JCR type manager
     * @param registry
     * @throws RepositoryException if there is a problem importing the types
     */
    private void importTypes(List<Tree<ObjectType>> types,
                             NodeTypeManager typeManager,
                             NamespaceRegistry registry,
                             List<NodeTypeTemplate> typeTemplates) throws RepositoryException {
        for (Tree<ObjectType> tree : types) {
            importType(tree.getItem(), typeManager, registry, typeTemplates);
            importTypes(tree.getChildren(), typeManager, registry, typeTemplates);
        }
    }

    /**
     * Indicates what given cmis base type can be imported
     *
     * @param baseTypeId cmis base type id
     * @return true   if base type will be imported
     *         false  otherwise
     */
    public static boolean isSupportedBaseType(BaseTypeId baseTypeId) {
        return ((baseTypeId == BaseTypeId.CMIS_DOCUMENT) || (baseTypeId == BaseTypeId.CMIS_FOLDER));
    }


    /**
     * Import given CMIS type to the JCR repository.
     *
     * @param cmisType    cmis object type
     * @param typeManager JCR type manager/
     * @param registry    jcr namespace registry
     * @throws RepositoryException if there is a problem importing the types
     */
    @SuppressWarnings("unchecked")
    public void importType(ObjectType cmisType,
                           NodeTypeManager typeManager,
                           NamespaceRegistry registry,
                           List<NodeTypeTemplate> typeTemplates) throws RepositoryException {
        // cache
        cachedTypeDefinitions.put(cmisType.getId(), cmisType);

        // skip base types because we are going to
        // map base types directly
        if (cmisType.isBaseType() || !isSupportedBaseType(cmisType.getBaseTypeId())) {
            return;
        }

        if (cmisType.getId().equals(Constants.CMIS_DOCUMENT_UNVERSIONED)) {
            return;
        }

        MappedCustomType mapping = mappedTypes.findByExtName(cmisType.getId());
        String cmisTypeId = mapping.getJcrName();

        // namespace registration
        debug("Type: ", cmisTypeId);
        if (!cmisTypeId.equals(cmisType.getLocalName()) && cmisTypeId.contains(":")) {

            String nsPrefix = cmisTypeId.substring(0, cmisTypeId.indexOf(":"));
            String nsUri = mapping.isTransient() ? cmisType.getLocalNamespace() : mapping.getJcrNamespaceUri();
            debug("check type namespace type: ", nsPrefix, ":", nsUri);
            // check is ns is not registered already with exactly same prefix and uri
            // if one of items presents typeManager should throw an exception while registering
            if (!isNsAlreadyRegistered(cmisType, registry, nsPrefix, nsUri)) {
                debug("register namespace type: ", nsPrefix, ":", nsUri);
                registry.registerNamespace(nsPrefix, nsUri);
            }
            prefixes.addNamespace(nsPrefix, nsUri);
        }


        // create node type template
        NodeTypeTemplate type = typeManager.createNodeTypeTemplate();

        // convert CMIS type's attributes to node type template we have just created
        type.setName(mapping.getJcrName());
        type.setAbstract(!cmisType.isCreatable());
        type.setMixin(false);
        type.setOrderableChildNodes(true);
        type.setQueryable(true);
        type.setDeclaredSuperTypeNames(superTypes(cmisType));

        Map<String, PropertyDefinition<?>> props = cmisType.getPropertyDefinitions();
        Set<String> names = props.keySet();
        // properties
        for (String name : names) {
//            debug("importing property: ", name, " ...");
            if (name.startsWith(Constants.CMIS_PREFIX))
                continue; // ignore them. they must be handled/mapped with default logic
            if (mapping.isIgnoredExtProperty(name)) continue; // explicit ignore

            PropertyDefinition<?> cmisPropDef = props.get(name);
            PropertyDefinitionTemplate jcrProp = typeManager.createPropertyDefinitionTemplate();
            jcrProp.setName(mapping.toJcrProperty(name));

            jcrProp.setMandatory(cmisPropDef.isRequired());
            jcrProp.setRequiredType(properties.getJcrType(cmisPropDef.getPropertyType()));
            jcrProp.setMultiple(cmisPropDef.getCardinality().equals(Cardinality.MULTI));
            jcrProp.setProtected(cmisPropDef.getUpdatability() == Updatability.READONLY);
            jcrProp.setAvailableQueryOperators(new String[]{});
            jcrProp.setAutoCreated(false);

            // if property is protected and already declared in parents - ignore it
            // we cannot override protected property so try to avoid it
            if (jcrProp.isProtected()/* && parentsHasPropertyDeclared(typeManager, type, jcrProp)*/) {
                continue;
            }

            if (cmisPropDef.getChoices() != null && cmisPropDef.getChoices().size() > 0) {
                LinkedList<String> choices = new LinkedList<String>();

                for (Choice choice : cmisPropDef.getChoices()) {
                    if (choice.getValue() != null && choice.getValue().size() > 0) {
                        Object choiceValue = choice.getValue().get(0);

                        if (choiceValue instanceof String) {
                            choices.add((String) choiceValue);
                        } else {
                            choices.add(choiceValue.toString());
                        }
                    }
                }

                jcrProp.setValueConstraints(choices.toArray(new String[choices.size()]));
            }

            if (!jcrProp.isProtected()) {
                final String extPropertyName = mapping.toExtProperty(jcrProp.getName());
                final boolean globalIgnored = globalIgnoredExtProperties.contains(extPropertyName);
                final boolean ignoredByType = mapping.isIgnoredExtProperty(jcrProp.getName());
                if (mapping != null && !globalIgnored && !ignoredByType) {
                    registeredProperties.put(extPropertyName, jcrProp.getName());
                    type.getPropertyDefinitionTemplates().add(jcrProp);
                }
            }
        }

        /*// todo add check if already added
        if ("notifications:hixDocument".equals(mapping.getJcrName())) {
            PropertyDefinitionTemplate jcrExtId = typeManager.createPropertyDefinitionTemplate();
            jcrExtId.setName("notifications:extId");

            jcrExtId.setMandatory(false);
            jcrExtId.setAutoCreated(true);

//            Object val = factories.getValueFactory(PropertyType.STRING).create("none");
//            Value[] values = {new Value(factories, PropertyType.STRING, val)};

//            jcrExtId.setDefaultValues(values);


            jcrExtId.setRequiredType(javax.jcr.PropertyType.STRING);
            jcrExtId.setMultiple(false);
            jcrExtId.setProtected(false);
            jcrExtId.setAvailableQueryOperators(new String[]{});
            type.getPropertyDefinitionTemplates().add(jcrExtId);
        }*/

        typeTemplates.add(type);

        Name jcrName = factories.getNameFactory().create(type.getName());
        MappedCustomType mappedType = mappedTypes.findByExtName(mapping.getExtName());
        // put a duplicate with another variant of JCR name like {namespae}localname
        mappedTypes.addSecondaryJcrKey(jcrName.toString(), mappedType);
    }

    /*
    * update existent types in typeManager with new features
    */
    public void updateTypes(NodeTypeManager typeManager, List<NodeTypeTemplate> defList) throws RepositoryException {
        if (mappedTypes == null)
            return;

        if (typeManager == null)
            return;

        for (String typeKey : mappedTypes.indexByJcrName.keySet()) {
            MappedCustomType mcType = mappedTypes.findByJcrName(typeKey);
            //  Enabling SNS
            if (mcType.hasFeature(Constants.FEATURE_NAME_SNS)) {
                NodeDefinitionTemplate child = typeManager.createNodeDefinitionTemplate();
                child.setName("*");
                String baseTypeName = mcType.getFeature(Constants.FEATURE_NAME_SNS);

                child.setRequiredPrimaryTypeNames(new String[]{baseTypeName});
                child.setSameNameSiblings(true);

                // Obtain type definition - and update it
                boolean foundInDefList = false;
                for (NodeTypeTemplate defType : defList) {
                    if (defType.getName().equals(typeKey)) {
                        foundInDefList = true;
                        defType.getNodeDefinitionTemplates().add(child);
                    }
                }
                if (foundInDefList) {
                    continue;  // was already updated in definition list
                }

                NodeType type = null;
                try {
                    type = typeManager.getNodeType(typeKey);
                } catch (NoSuchNodeTypeException e) {
                    continue;   // no such type registered
                }
                if (type == null)
                    continue;
                NodeTypeTemplate typeTemplate = typeManager.createNodeTypeTemplate(type);
                typeTemplate.getNodeDefinitionTemplates().add(child);
                // Type was obtained from type manager, so update it there
                NodeTypeDefinition[] nodeDefs = new NodeTypeDefinition[]{typeTemplate};
                typeManager.registerNodeTypes(nodeDefs, true);
            }
        }
    }

    /*
    * looking for protected property through node's parents` definitions
    */
    public boolean parentsHasPropertyDeclared(javax.jcr.nodetype.NodeTypeManager typeManager, NodeTypeDefinition typeDef, PropertyDefinitionTemplate pt) {
        for (String sType : typeDef.getDeclaredSupertypeNames()) {
            try {
                NodeType nodeType = typeManager.getNodeType(sType);
                javax.jcr.nodetype.PropertyDefinition[] propertyDefinitions = nodeType.getPropertyDefinitions();
                for (javax.jcr.nodetype.PropertyDefinition propertyDefinition : propertyDefinitions) {
                    if (propertyDefinition.getName().equals(pt.getName()) && propertyDefinition.isProtected())
                        return true;
                }
            } catch (RepositoryException ignore) {/**/}
        }

        return false;
    }

    private boolean isNsAlreadyRegistered(ObjectType cmisType, NamespaceRegistry registry, String nsPrefix, String nsUri) throws RepositoryException {
        if (ArrayUtils.contains(registry.getPrefixes(), nsPrefix) && ArrayUtils.contains(registry.getURIs(), nsUri))
            return true;

        if (cmisType != null && StringUtils.equals(cmisType.getBaseType().getLocalNamespace(), cmisType.getLocalNamespace()))
            return true;

        return false;
    }

    /**
     * Determines supertypes for the given CMIS type in terms of JCR.
     *
     * @param cmisType given CMIS type
     * @return supertypes in JCR lexicon.
     */
    private String[] superTypes(ObjectType cmisType) {
        String parentType = (cmisType.getParentType() != null) ?
                getJcrTypeId(cmisTypeToJcr(cmisType.getParentType().getId()).getJcrName())
                : null;

        if (parentType == null) {
            if (cmisType.getBaseTypeId() == BaseTypeId.CMIS_FOLDER) {
                parentType = JcrConstants.NT_FOLDER;
            } else if (cmisType.getBaseTypeId() == BaseTypeId.CMIS_DOCUMENT) {
                parentType = JcrConstants.NT_FILE;
            }
        }

        return addMixins(cmisType, new String[]{parentType});
    }


    /*
    * supposed to add mix:versionable if cmis types is a VersionableDocument
    * there used to be versionable notation but versioning support for connectors is not there yet
    */
    protected String[] addMixins(ObjectType cmisType, String[] superTypes) {
        return superTypes;
    }


    /*
    * replace direct CMIS types with JCR equivalents
    */
    private String getJcrTypeId(String cmisTypeId) {
        if (cmisTypeId == null) return null;

        if (cmisTypeId.equals(BaseTypeId.CMIS_DOCUMENT.value())) {
            return JcrConstants.NT_FILE;
        } else if (cmisTypeId.equals(BaseTypeId.CMIS_FOLDER.value())) {
            return JcrConstants.NT_FOLDER;
        }

        return cmisTypeId;
    }

    /**
     * Defines node type for the repository info.
     *
     * @param typeManager JCR node type manager.
     * @throws RepositoryException
     */
    @SuppressWarnings("unchecked")
    private void registerRepositoryInfoType(NodeTypeManager typeManager) throws RepositoryException {
        // create node type template
        NodeTypeTemplate type = typeManager.createNodeTypeTemplate();

        // convert CMIS type's attributes to node type template we have just created
        type.setName("cmis:repository");
        type.setAbstract(false);
        type.setMixin(false);
        type.setOrderableChildNodes(true);
        type.setQueryable(true);
        type.setDeclaredSuperTypeNames(new String[]{JcrConstants.NT_FOLDER});

        PropertyDefinitionTemplate vendorName = typeManager.createPropertyDefinitionTemplate();
        vendorName.setAutoCreated(false);
        vendorName.setName("cmis:vendorName");
        vendorName.setMandatory(false);

        type.getPropertyDefinitionTemplates().add(vendorName);

        PropertyDefinitionTemplate productName = typeManager.createPropertyDefinitionTemplate();
        productName.setAutoCreated(false);
        productName.setName("cmis:productName");
        productName.setMandatory(false);

        type.getPropertyDefinitionTemplates().add(productName);

        PropertyDefinitionTemplate productVersion = typeManager.createPropertyDefinitionTemplate();
        productVersion.setAutoCreated(false);
        productVersion.setName("cmis:productVersion");
        productVersion.setMandatory(false);

        type.getPropertyDefinitionTemplates().add(productVersion);

        // register type
        NodeTypeDefinition[] nodeDefs = new NodeTypeDefinition[]{type};
        typeManager.registerNodeTypes(nodeDefs, true);
    }


    /// --------------------------- logging ------------------------

    private void logMappedTypes() {
        for (MappedCustomType mapping : getMappedTypes().mappings) {
            debug("Mapping: jcr/cmisExt <" + mapping.getJcrName() + "> = <" + mapping.getExtName() + ">");
            debug("Mapped Properties..");
            for (Map.Entry<String, String> entry : mapping.indexJcrProperties.entrySet()) {
                debug("mapped property jct/cmisExt <" + entry.getKey() + "> = <" + entry.getValue() + ">");
            }
            StringBuilder ignoredPropsString = new StringBuilder();
            if (mapping.getIgnoreExternalProperties() != null)
                for (String s : mapping.getIgnoreExternalProperties()) {
                    ignoredPropsString.append(s).append(";");
                }
            debug("Ignored external Properties: " + ignoredPropsString.toString());
            debug("end mapping info -----");
        }
    }


    // DEBUG. this method will be removed todo
    public void debug(String... values) {
        if (debug) {
            StringBuilder stringBuilder = new StringBuilder();
            for (String value : values) {
                stringBuilder.append(value).append(" ");
            }
            LOG.debug(stringBuilder.toString());
        }
    }

    /*
     * find appropriate mapped type or return input back
    */
    public MappedCustomType cmisTypeToJcr(String cmisTypeId) {
        return mappedTypes.findByExtName(cmisTypeId);
    }

    /*
    * find appropriate mapped type or return input back
    */
    public MappedCustomType jcrTypeToCmis(String jcrName) {
        return mappedTypes.findByJcrName(jcrName);

    }


    public Map<String, String> getRegisteredProperties() {
        return registeredProperties;
    }
}
