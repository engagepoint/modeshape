package org.modeshape.connector.cmis;

import java.util.HashMap;
import java.util.Map;

public class MappedCustomType {

    private String jcrNamespaceUri;
    private String[] ignoreExternalProperties;
	private String jcrName;
    private String extName;
    
    Map<String, String> indexJcrProperties = new HashMap<String, String>();
    Map<String, String> indexExtProperties = new HashMap<String, String>();

    public MappedCustomType(String jcrName, String extName) {
        this.jcrName = jcrName;
        this.extName = extName;
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
    
    public String[] getIgnoreExternalProperties() {
		return ignoreExternalProperties;
	}

    public void addPropertyMapping(String jcrName, String extName) {
        indexJcrProperties.put(jcrName, extName);
        indexExtProperties.put(extName, jcrName);
    }

    public String toJcrProperty(String cmisName) {
        String result = indexExtProperties.get(cmisName);
        return result != null ? result : cmisName;
    }

    public String toExtProperty(String jcrName) {
        String result = indexJcrProperties.get(jcrName);
        return result != null ? result : jcrName;
    }

    public void setJcrNamespaceUri(String jcrNamespaceUri) {
        this.jcrNamespaceUri = jcrNamespaceUri;
    }
    
    public void setIgnoreExternalProperties(String ignoreExternalProps) {
    	this.ignoreExternalProperties = ignoreExternalProps.split(",");
    }
}
