package org.modeshape.jcr;

import org.infinispan.schematic.DocumentFactory;
import org.infinispan.schematic.document.Document;
import org.infinispan.schematic.internal.document.BasicDocument;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author evgeniy.shevchenko
 * @version 1.0 4/18/2017
 */

public class BackupDocumentWriterUtil {
    private static final String ROOT_REGEX = "^[0-9a-fA-F]{14}\\/$";
    private static final Pattern ROOT_PATTERN = Pattern.compile(ROOT_REGEX);

    private static final String UUID_REGEX = "^[0-9a-fA-F]{14}[0-9a-fA-F]{8}-?[0-9a-fA-F]{4}-?[0-9a-fA-F]{4}-?[0-9a-fA-F]{4}-?[0-9a-fA-F]{12}$";
    private static final Pattern UUID_PATTERN = Pattern.compile(UUID_REGEX);
    private static final String JCRUNFILED_REGEX = "^[0-9a-fA-F]{14}\\{http\\:\\/\\/www\\.jcp\\.org\\/jcr\\/1\\.0\\}unfiled$";
    private static final Pattern JCRUNFILED_PATTERN = Pattern.compile(JCRUNFILED_REGEX);

    public static boolean isContentMatches(Document doc, String custom) {
        if (doc.containsField("content")) {
            Document metadata = doc.getDocument("content");
            if (metadata.containsField("properties")) {
                Document properties = metadata.getDocument("properties");
                if (properties.containsField("http://www.jcp.org/jcr/1.0")) {
                    Document jcr = properties.getDocument("http://www.jcp.org/jcr/1.0");
                    if (jcr.containsField("primaryType")) {
                        Document primaryType = jcr.getDocument("primaryType");
                        if (primaryType.containsField("$name")) {
                            String name = primaryType.getString("$name");
                            return custom.equals(name);
                        }
                    }
                }
            }

        }
        return false;
    }

    public static boolean isNamespaceNode(Document doc) {
        return isContentMatches(doc, "mode:namespace");
    }

    public static boolean isNamespacesNode(Document doc) {
        return isContentMatches(doc, "mode:namespaces");
    }

    public static boolean isSystemNode(Document doc) {
        return isContentMatches(doc, "mode:system");
    }

    public static boolean isRoot(Document doc) {
        if (isMetadataMatches(doc, ROOT_PATTERN)) {
            Document rootContent = doc.getDocument("content");
            Document rootChildrenInfo = rootContent.getDocument("childrenInfo");
            return rootChildrenInfo.getLong("count")>1;
        }
        return false;
    }

    private static String extractDocumentId(Document doc) {
        Document metadata = doc.getDocument("metadata");
        return metadata.getString("id");
    }

    public static boolean isMetadataMatches(Document doc, Pattern pattern) {
        if (doc.containsField("metadata")) {
            String id = extractDocumentId(doc);
            Matcher matcher = pattern.matcher(id);
            return matcher.matches();
        }
        return false;
    }

    public static boolean isUnfiledFolder(Document doc) {
        return isMetadataMatches(doc, JCRUNFILED_PATTERN);
    }
    private static final List<String> IGNORE_PRIMARY_TYPE_NAMES = Arrays.asList("mode:versionHistoryFolder","nt:versionLabels","nt:versionHistory","nt:frozenNode","nt:version");

    public static boolean isDocumentNode(Document doc, boolean checkIgnored) {
        boolean idMatches = isMetadataMatches(doc, UUID_PATTERN);
        if (idMatches && checkIgnored) {
            if (doc.containsField("content")) {
                String primaryTypeName = extractPrimaryType(doc);
                return primaryTypeName!=null && !IGNORE_PRIMARY_TYPE_NAMES.contains(primaryTypeName);
            }
        }
        return  idMatches;
    }

    private static Document extractJcrProperties(Document doc) {
        if (doc.containsField("content")) {
            Document content = doc.getDocument("content");
            if (content.containsField("properties")) {
                Document properties = content.getDocument("properties");
                if (properties.containsField("http://www.jcp.org/jcr/1.0")) {
                    return properties.getDocument("http://www.jcp.org/jcr/1.0");
                }
            }
        }
        return null;
    }

    private static String extractPrimaryType(Document doc) {
        Document jcrProperties = extractJcrProperties(doc);
        if (null != jcrProperties) {
            Document primaryType = jcrProperties.getDocument("primaryType");
            if (primaryType.containsField("$name")) {
                return primaryType.getString("$name");
            }
        }

        return null;
    }
    public static boolean isDocumentContentNode(Document doc) {
        boolean idMatches = isDocumentNode(doc, false);
        if (idMatches && doc.containsField("content")) {
            String primaryTypeName = extractPrimaryType(doc);
            return "nt:resource".equals(primaryTypeName) || "nt:version".equals(primaryTypeName);
        }
        return false;
    }

    public static String getJcrContentId(Document doc) {
        boolean matches = isDocumentNode(doc, Boolean.FALSE);
        if (matches) {
            Document content = doc.getDocument("content");
            if (content.containsField("children")) {
                List<Document> children = (List<Document>) content.getArray("children");
                for(Document item : children) {
                    String name = item.getString("name");
                    if ("jcr:content".equals(name)) {
                        return item.getString("key");
                    }
                }
            }
        }
        return null;
    }

    public static boolean isUnfiledDocument(Document doc) {
        if (doc.containsField("content")) {
            Document content = doc.getDocument("content");
            if (content.containsField("parent")) {
                List ids = content.getArray("parent");
                if (null != ids) {
                    for (Object id : ids) {
                        Matcher matcher = JCRUNFILED_PATTERN.matcher(id.toString());
                        if (matcher.matches()) {
                            return true;
                        }
                    }
                } else {
                    String id = content.getString("parent");
                    Matcher matcher = JCRUNFILED_PATTERN.matcher(id);
                    return matcher.matches();
                }
            }
        }
        return false;
    }

    public static Document filterChildren(Document source, Collection<String> excludeNames) {
        return appendChildren(source, Collections.EMPTY_LIST, excludeNames);
    }

    public static Document appendChildren(Document source, List<Document> newChildren) {

        return appendChildren(source, newChildren, Collections.EMPTY_LIST);
    }

    public static Document appendChildren(Document source, List<Document> newChildren, Collection<String> excludeNames) {
        Document content = source.getDocument("content");
        Document childrenInfo = content.getDocument("childrenInfo");
        List<Document> children = new ArrayList<Document>();
        if (content!=null && content.containsField("children")) {
            List<Document> oldChildren = (List<Document>) content.getArray("children");
            for (Document item : oldChildren) {
                String name = item.getString("name");
                if (null != name && !excludeNames.contains(name)) {
                    children.add(item);
                }
            }
        }
        for (Document item : newChildren) {
            children.add(item);
        }
        Document updatedContent =  content
                .with("childrenInfo", childrenInfo.with("count", children.size()))
                .with("children", children);
        return source.with("content", updatedContent);
    }

    public static String extractDocumentKey(Document document){
        Document content = document.getDocument("content");
        return content.getString("key").substring(0, 14);
    }

    public static String extractDocumentParentKey(Document document){
        Document content = document.getDocument("content");
        return content.getString("parent");
    }

    public static Document createDocWithKeyAndName(String key, String name) {
        Document item = new BasicDocument();
        ((BasicDocument)item).put("key", key);
        ((BasicDocument)item).put("name", name);
        return item;
    }

    public static Document updateParentForNode(Document doc, String parent) {
        Document content = doc.getDocument("content");
        return doc.with("content", content.with("parent", parent));
    }

    private static Document updateDocProperty(Document doc, String propName, Object value) {
        if (doc.containsField(propName)) {
            return doc.with(propName, value);
        }
        BasicDocument.class.cast(doc).put(propName, value);
        return doc;
    }
    public static Document fixLastModified(Document doc, Document jcrContent) {
        if (null != jcrContent && isDocumentNode(doc, Boolean.FALSE)) {
            Document jcrProps = extractJcrProperties(jcrContent);
            if (null != jcrProps) {
                Document lastModified = jcrProps.getDocument("lastModified");
                String lastModifiedBy = jcrProps.getString("lastModifiedBy");
                if (lastModified != null && lastModified.containsField("$date") &&
                        null != lastModifiedBy) {
                    Document content = doc.getDocument("content");
                    Document properties = content.getDocument("properties");

                    Document docJcrProps = extractJcrProperties(doc);
                    List<Document> mixinTypes = new ArrayList<Document>();
                    if (docJcrProps.containsField("mixinTypes")) {
                        mixinTypes.addAll((List<Document>) docJcrProps.getArray("mixinTypes"));
                    }
                    mixinTypes.add(new BasicDocument("$name", "mix:lastModified"));
                    docJcrProps = updateDocProperty(docJcrProps, "mixinTypes", DocumentFactory.newArray(mixinTypes));
                    docJcrProps = updateDocProperty(docJcrProps, "lastModified", lastModified);
                    docJcrProps = updateDocProperty(docJcrProps, "lastModifiedBy", lastModifiedBy);
                    Document updatedContent = content.with("properties", properties.with("http://www.jcp.org/jcr/1.0", docJcrProps));
                    return doc.with("content", updatedContent);
                }
            }

        }

        return doc;
    }
}
