/*
 * ModeShape (http://www.modeshape.org)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.modeshape.jcr;

import org.infinispan.schematic.SchematicEntry;
import org.infinispan.schematic.document.Document;
import org.infinispan.schematic.internal.document.BasicDocument;
import org.modeshape.jcr.cache.document.LocalDocumentStore;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author evgeniy.shevchenko
 * @version 1.0 4/18/2017
 */

public class RestoreDocumentUtil {
    private static final String ROOT_REGEX = "^[0-9a-fA-F]{14}\\/$";
    private static final Pattern ROOT_PATTERN = Pattern.compile(ROOT_REGEX);


    private static final String INDEX_PROVIDER_REGEX = "^[0-9a-fA-F]{14}\\/jcr:system\\/jcr\\:nodeTypes\\/mode\\:indexProvider.*";
    private static final Pattern INDEX_PROVIDER_PATTERN = Pattern.compile(INDEX_PROVIDER_REGEX);

    private static final String MODE_INDEXES_REGEX = "^[0-9a-fA-F]{14}\\/jcr:system\\/jcr\\:nodeTypes\\/mode\\:indexes.*";
    private static final Pattern MODE_INDEXES_PATTERN = Pattern.compile(MODE_INDEXES_REGEX);



    public static boolean isMetadataMatches(Document doc, Pattern pattern) {
        if (doc.containsField("metadata")) {
            Document metadata = doc.getDocument("metadata");
            String id = metadata.getString("id");
            Matcher matcher = pattern.matcher(id);
            return matcher.matches();
        }
        return false;
    }

    public static boolean isIndexProviderNode(Document doc) {
        return isMetadataMatches(doc, INDEX_PROVIDER_PATTERN);
    }

    public static boolean isModeIndexesNode(Document doc) {
        return isMetadataMatches(doc, MODE_INDEXES_PATTERN);
    }


    public static String extractDocumentId(Document document){
        Document content = document.getDocument("metadata");
        return content.getString("id");
    }

    public static String extractDocumentKey(Document document){
        Document content = document.getDocument("content");
        return content.getString("key");
    }

    public static String extractDocumentParent(Document document){
        Document content = document.getDocument("content");
        return content.getString("parent");
    }

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

    public static boolean isSystemNode(Document doc) {
        return isContentMatches(doc, "mode:system");
    }


    private static Document updateMetadata(Document doc, String rootPrefix) {
        if (doc.containsField("metadata")) {
            Document metadata = doc.getDocument("metadata");
            String oldId = extractDocumentId(doc);
            String newId = rootPrefix + oldId.substring(14);
            return metadata.with("id", newId);
        } else {
            return null;
        }
    }

    private static List<Document> updateChildren(Document content, String rootPrefix) {
        if (content.containsField("children")) {
            List<Document> children = (List<Document>) content.getArray("children");
            List<Document> updatedChildren = new ArrayList<>();
            for (Document item : children) {
                String oldKey = item.getString("key");
                String newKey = rootPrefix + oldKey.substring(14);
                updatedChildren.add(item.with("key", newKey));
            }
            return updatedChildren;
        } else {
            return null;
        }
    }

    private static Document updateContent(Document doc, String rootPrefix) {
        if (doc.containsField("content")) {
            Document content = doc.getDocument("content");
            String oldKey = extractDocumentKey(doc);
            String oldParent = extractDocumentParent(doc);
            String newKey = rootPrefix + oldKey.substring(14);
            String newParent = rootPrefix + oldParent.substring(14);
            Document updatedContent = content
                    .with("key", newKey)
                    .with("parent", newParent)
                    .with("children", updateChildren(content, rootPrefix));
            return updatedContent;
        } else {
            return null;
        }
    }

    public static Document updateRootPrefix(Document doc, String rootPrefix) {
        return doc
                .with("metadata", updateMetadata(doc, rootPrefix))
                .with("content", updateContent(doc, rootPrefix));

    }

    public static Document findSystemNode(final LocalDocumentStore documentStore) {
        for(Map.Entry<String, SchematicEntry> entry :  documentStore.localCache().entrySet()) {
            Document doc = entry.getValue().asDocument();
            if (null != doc) {
                if (isSystemNode(doc)) {
                    return doc;
                }
            }
        }
        throw new IllegalStateException("Couldn't find system node!");
    }

    public static List<Document> extractNodesToMove(final LocalDocumentStore documentStore) {
        List<Document> additionalDocs = new ArrayList<Document>();
        for(Map.Entry<String, SchematicEntry> entry :  documentStore.localCache().entrySet()) {
            Document doc = entry.getValue().asDocument();
            if (null != doc) {
                if (isIndexProviderNode(doc) || isModeIndexesNode(doc)) {
                    additionalDocs.add(doc);
                }
            }
        }

        return additionalDocs;
    }

    public static Document appendChildren(Document source, List<Document> newChildren) {

        return appendChildren(source, newChildren, Collections.EMPTY_LIST);
    }

    public static Document createDocWithKeyAndName(String key, String name) {
        Document item = new BasicDocument();
        ((BasicDocument)item).put("key", key);
        ((BasicDocument)item).put("name", name);
        return item;
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

}
