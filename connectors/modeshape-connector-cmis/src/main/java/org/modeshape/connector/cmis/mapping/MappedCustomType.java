package org.modeshape.connector.cmis.mapping;

import java.util.*;

public class MappedCustomType {

    private boolean isTransient = false;

    private String jcrNamespaceUri;
    private Set<String> ignoreExternalProperties = new HashSet<String>();
    private String jcrName;
    private String extName;

    Map<String, String> indexJcrProperties = new HashMap<String, String>();
    Map<String, String> indexExtProperties = new HashMap<String, String>();

    private MappedCustomType defaultsDelegate = null;

    public MappedCustomType(String typeName, MappedCustomType defaultDelegate) {
        this.jcrName = typeName;
        this.extName = typeName;
        this.isTransient = true;
        this.defaultsDelegate = defaultDelegate;
    }

    public MappedCustomType(String jcrName, String extName) {
        this.jcrName = jcrName;
        this.extName = extName;
    }

    public MappedCustomType(String jcrName, String extName, MappedCustomType defaultsDelegate) {
        this.jcrName = jcrName;
        this.extName = extName;
        this.defaultsDelegate  = defaultsDelegate;
    }

    public String getJcrName() {
        return jcrName;
    }

    public String getExtName() {
        return extName;
    }

    public String getJcrNamespaceUri() {
        return jcrNamespaceUri;
    }

    public Collection<String> getIgnoreExternalProperties() {
        return ignoreExternalProperties;
    }

    public boolean isIgnoredExtProperty(String extProperty) {
        if (ignoreExternalProperties.contains(extProperty))
            return true;

        if (defaultsDelegate != null)
            return defaultsDelegate.isIgnoredExtProperty(extProperty);

        return false;
    }

    public void addPropertyMapping(String jcrName, String extName) {
        indexJcrProperties.put(jcrName, extName);
        indexExtProperties.put(extName, jcrName);
    }

    public String toJcrProperty(String cmisName) {
        if (isIgnoredExtProperty(cmisName))
            return null;

        String result = indexExtProperties.get(cmisName);

        if (defaultsDelegate != null)
            return defaultsDelegate.toJcrProperty(cmisName);

        return result != null ? result : cmisName;
    }

    public String toExtProperty(String jcrName) {
        String result = indexJcrProperties.get(jcrName);

        if (defaultsDelegate != null)
            return defaultsDelegate.toExtProperty(jcrName);

        return result != null ? result : jcrName;
    }

    public void setJcrNamespaceUri(String jcrNamespaceUri) {
        this.jcrNamespaceUri = jcrNamespaceUri;
    }

    public void setIgnoreExternalProperties(Iterable<String> ignoreExternalProps) {
        for (String sType : ignoreExternalProps) {
            this.ignoreExternalProperties.add(sType.trim());
        }
    }

    public boolean isTransient() {
        return isTransient;
    }

}
