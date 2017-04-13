package org.modeshape.jcr;

import org.infinispan.schematic.SchematicEntry;
import org.infinispan.schematic.document.Binary;
import org.infinispan.schematic.document.Document;
import org.infinispan.schematic.internal.document.BasicDocument;
import org.modeshape.common.collection.Problems;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author evgeniy.shevchenko
 * @version 1.0 4/13/2017
 */

public class BackupDocumentWriterWrapper  {
    private final BackupDocumentWriter backupDocumentWriter;
    private static final String UUID_REGEX = "^[0-9a-fA-F]{14}[0-9a-fA-F]{8}-?[0-9a-fA-F]{4}-?[0-9a-fA-F]{4}-?[0-9a-fA-F]{4}-?[0-9a-fA-F]{12}$";
    private static final Pattern UUID_PATTERN = Pattern.compile(UUID_REGEX);
    private static final String JCRUNFILED_REGEX = "^[0-9a-fA-F]{14}\\{http\\:\\/\\/www\\.jcp\\.org\\/jcr\\/1\\.0\\}unfiled$";
    private static final Pattern JCRUNFILED_PATTERN = Pattern.compile(JCRUNFILED_REGEX);
    HashSet<String> binaryKeys = new HashSet<String>();
    private final File journal;
    private Writer writer;
    public BackupDocumentWriterWrapper(final File backupDirectory, BackupDocumentWriter backupDocumentWriter) {
        this.backupDocumentWriter = backupDocumentWriter;
        journal = new File(backupDirectory, "journal.csv");

    }

    void initJournalWriter() throws IOException {
        writer =  new FileWriter(journal);
    }

    void closeJournalWriter() throws IOException {
        if (null != writer) {
            writer.close();
        }
    }
    private Map<String, String> namespaces = new HashMap<String, String>(){{
        put("jcr", "mode:namespaces-http://www.jcp.org/jcr/1.0");
        put("nt", "mode:namespaces-http://www.jcp.org/jcr/nt/1.0");
        put("mix", "mode:namespaces-http://www.jcp.org/jcr/mix/1.0");
        put("sv", "mode:namespaces-http://www.jcp.org/jcr/sv/1.0");
        put("mode", "mode:namespaces-http://www.modeshape.org/1.0");
        put("xml", "mode:namespaces-http://www.w3.org/XML/1998/namespace");
        put("xmlns", "mode:namespaces-http://www.w3.org/2000/xmlns/");
        put("xs", "mode:namespaces-http://www.w3.org/2001/XMLSchema");
        put("xsi", "mode:namespaces-http://www.w3.org/2001/XMLSchema-instance");
    }};

    private Document updateSystemFolder(Document systemFolder) {

        Document content = systemFolder.getDocument("content");
        Document childrenInfo = content.getDocument("childrenInfo");
        List<Document> children = new ArrayList<Document>();
        if (content!=null && content.containsField("children")) {
            List<Document> oldChildren = (List<Document>) content.getArray("children");
            for (Document item : oldChildren) {
                String name = item.getString("name");
                if (null != name && !"jcr:unfiled".equals(name)) {
                    children.add(item);
                }
            }
        }
        Document updatedContent = content
                .with("childrenInfo", childrenInfo.with("count", children.size()))
                .with("children", children);
        return  systemFolder.with("content", updatedContent);
    }

    private boolean isSystem(Document doc) {
        return isContentMatches(doc, "mode:system");
    }

    private boolean isMetadataMatches(Document doc, Pattern pattern) {
        if (doc.containsField("metadata")) {
            Document metadata = doc.getDocument("metadata");
            String id = metadata.getString("id");
            Matcher matcher = pattern.matcher(id);
            return matcher.matches();
        }
        return false;
    }

    public void write(Document doc) throws IOException {
        processDoc(doc);
        if (isMetadataMatches(doc, JCRUNFILED_PATTERN)) {
            Document content = doc.getDocument("content");
            Document childrenInfo = content.getDocument("childrenInfo");
            List<Document> children = new ArrayList<Document>();
            if (content!=null && content.containsField("children")) {
                List<Document> oldChildren = (List<Document>) content.getArray("children");
                for (Document item : oldChildren) {
                    String name = item.getString("name");
                    if (!namespaces.containsKey(name)) {
                        children.add(item);
                    }
                }
                Document updatedContent =  content
                        .with("childrenInfo", childrenInfo.with("count", children.size()))
                        .with("children", children);
                doc = doc.with("content", updatedContent);
            }

        } else if (isNamespaces(doc)) {
            Document content = doc.getDocument("content");
            String key = content.getString("key").substring(0, 14);
            Document childrenInfo = content.getDocument("childrenInfo");
            List<Document> children = new ArrayList<Document>();
            if (content!=null && content.containsField("children")) {
                List<Document> oldChildren = (List<Document>) content.getArray("children");
                for (Document item : oldChildren) {
                    children.add(item);
                }
            }
            for (Map.Entry<String, String> entry : namespaces.entrySet()) {
                Document item = new BasicDocument();
                ((BasicDocument)item).put("key", key + entry.getValue());
                ((BasicDocument)item).put("name", entry.getKey());
                children.add(item);
            }
            Document updatedContent =  content
                    .with("childrenInfo", childrenInfo.with("count", children.size()))
                    .with("children", children);
            doc = doc.with("content", updatedContent);

        }
        else if (isNamespace(doc)) {
            Document content = doc.getDocument("content");
            String parent = content.getString("parent");
            doc = doc
                    .with("content",
                            content.with("parent",
                                    parent.substring(0, 14) + "mode:namespaces"));
        } else if (isSystem(doc)) {
            doc = updateSystemFolder(doc);
        }
        backupDocumentWriter.write(doc);
    }

    private boolean isContentMatches(Document doc, String custom) {
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

    private boolean isNamespace(Document doc) {
        return isContentMatches(doc, "mode:namespace");
    }

    private boolean isNamespaces(Document doc) {
        return isContentMatches(doc, "mode:namespaces");
    }


    public void close() {
        backupDocumentWriter.close();
    }

    public long getFileCount() {
        return backupDocumentWriter.getFileCount();
    }
    public long getDocumentCount(){
        return backupDocumentWriter.getDocumentCount();
    }

    private void processDoc(Document doc) throws IOException {
        if (isMetadataMatches(doc, UUID_PATTERN)) {
            StringBuilder builder = new StringBuilder();
            Document jcr = getJcrPropertiesDoc(doc);
            if (null !=jcr && jcr.containsField("primaryType")) {
                Document primaryType = jcr.getDocument("primaryType");
                if (primaryType.containsField("$name")) {
                    String name = primaryType.getString("$name");
                    if (!"mode:projection".equals(name)){
                        processMetadata(doc, builder);
                        processContent(jcr, builder);
                        String row = builder.toString();
                        if (!"".equals(row)) {
                            writer.append(row);
                            writer.append("\r\n");
                        }
                    }

                }
            }
        }
    }

    private Document getJcrPropertiesDoc(Document doc) {
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


    private void processMetadata(Document doc, StringBuilder builder) {
        if (doc.containsField("metadata")) {
            Document metadata = doc.getDocument("metadata");
            if (metadata.containsField("id")) {
                String id = metadata.getString("id");
                builder.append(id.substring(14));
                builder.append(",");
            }

        }
    }

    private void processContent(Document jcr, StringBuilder builder) {
        if (jcr.containsField("data")) {

            Binary dataAsBinary = jcr.getBinary("data");
            if (null != dataAsBinary) {
                builder.append(dataAsBinary.length());
                builder.append(",");
            } else {
                Document data = jcr.getDocument("data");
                if (null != data) {
                    if (data.containsField("$len")) {
                        Long len = data.getLong("$len");
                        builder.append(len);
                        builder.append(",");
                    }
                    if (data.containsField("$sha1")) {
                        String ref = data.getString("$sha1");
                        binaryKeys.add(ref);
                        builder.append(ref);
                        builder.append(",");
                    }
                }

            }


        }
    }

    public boolean containsBynaryKey(String binaryKey) {
        return binaryKeys.contains(binaryKey.toString());
    }

}
