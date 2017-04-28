package org.modeshape.jcr;

import org.infinispan.schematic.document.Array;
import org.infinispan.schematic.document.Document;
import org.modeshape.common.logging.Logger;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.modeshape.jcr.BackupDocumentWriterUtil.appendChildren;
import static org.modeshape.jcr.BackupDocumentWriterUtil.createDocWithKeyAndName;
import static org.modeshape.jcr.BackupDocumentWriterUtil.extractDocumentKey;
import static org.modeshape.jcr.BackupDocumentWriterUtil.extractDocumentParentKey;
import static org.modeshape.jcr.BackupDocumentWriterUtil.filterChildren;
import static org.modeshape.jcr.BackupDocumentWriterUtil.fixLastModified;
import static org.modeshape.jcr.BackupDocumentWriterUtil.isNamespaceNode;
import static org.modeshape.jcr.BackupDocumentWriterUtil.isNamespacesNode;
import static org.modeshape.jcr.BackupDocumentWriterUtil.isRoot;
import static org.modeshape.jcr.BackupDocumentWriterUtil.isSystemNode;
import static org.modeshape.jcr.BackupDocumentWriterUtil.isUnfiledDocument;
import static org.modeshape.jcr.BackupDocumentWriterUtil.isUnfiledFolder;
import static org.modeshape.jcr.BackupDocumentWriterUtil.updateParentForNode;
/**
 * @author evgeniy.shevchenko
 * @version 1.0 4/13/2017
 */

public class BackupDocumentWriterWrapper  {
    protected static final Logger LOGGER = Logger.getLogger(BackupDocumentWriterWrapper.class);

    private final BackupDocumentWriter backupDocumentWriter;
    private final BackupDocumentWriterJournal journal;

    private Document rootFolder;
    private Map<Document, Document> unfiledDocumentsCache = new HashMap<Document, Document>();
    private Document unfiledFolder;

    private Map<String, String> embeddedNamespaces = new HashMap<String, String>(){{
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

    public BackupDocumentWriterWrapper(final File backupDirectory, final BackupDocumentWriter backupDocumentWriter) {
        this.backupDocumentWriter = backupDocumentWriter;
        journal = new BackupDocumentWriterJournal(new File(backupDirectory, "journal.csv"));
    }

    void init() throws IOException {
        journal.init();
    }

    private Document excludeUnfiledNode(Document systemFolder) {
        return  filterChildren(systemFolder, Arrays.asList("jcr:unfiled"));
    }

    private Document insertUnfiledNodeIntoRoot() {
        return appendChildren(rootFolder, Arrays.asList(createDocWithKeyAndName(rootFolderKey().substring(0,14) + "jcr:unfiled", "jcr:unfiled")));
    }

    private Document excludeRedundantNodesFromUnfiledFolder() {
        Document doc = filterChildren(unfiledFolder, embeddedNamespaces.keySet());
        Document metadata = doc.getDocument("metadata");
        Document content = doc.getDocument("content");
        Document updated = doc
                .with(
                    "metadata", metadata.with("id", unfiledFolderKey()))
                .with(
                    "content", content.with("key", unfiledFolderKey()));
        return updateParentForNode(updated, rootFolderKey());
    }

    private String rootFolderKey(){
        if (null == rootFolder) {
            throw new IllegalStateException("Root folder is null");
        }
        return rootFolder.getDocument("metadata").getString("id");
    }

    private String unfiledFolderKey(){
        return rootFolderKey().substring(0, 14) + "jcr:unfiled";
    }
    
    private Document insertEmbeddedNamespaces(Document doc) {
        List<Document> items = new ArrayList<Document>();
        for (Map.Entry<String, String> entry : embeddedNamespaces.entrySet()) {
            items.add(createDocWithKeyAndName(extractDocumentKey(doc) + entry.getValue(), entry.getKey()));
        }
        return appendChildren(doc, items);
    }

    private Document fixNamespaceParentKey(Document doc) {
        return updateParentForNode(doc, extractDocumentParentKey(doc).substring(0, 14) + "mode:namespaces");
    }

    public void write(Document doc, Document jcrContent) throws IOException {
        if (isRoot(doc)) {
            rootFolder = doc;
        } else if (isUnfiledFolder(doc)) {
            unfiledFolder = doc;
        } else if (isNamespacesNode(doc)) {
            backupDocumentWriter.write(insertEmbeddedNamespaces(doc));
        } else if (isNamespaceNode(doc)) {
            backupDocumentWriter.write(fixNamespaceParentKey(doc));
        } else if (isSystemNode(doc)) {
            doc = excludeUnfiledNode(doc);
            backupDocumentWriter.write(doc);
        } else if (isUnfiledDocument(doc)) {
            if (null == rootFolder) {
                unfiledDocumentsCache.put(doc, jcrContent);
            } else {
                Document fixedLastModified = fixLastModified(doc, jcrContent);
                backupDocumentWriter.write(updateParentForNode(fixedLastModified, unfiledFolderKey()));
            }
            journal.addInfoToJournal(doc);
        } else {
            Document fixedLastModified = fixLastModified(doc, jcrContent);
            backupDocumentWriter.write(fixedLastModified);
            journal.addInfoToJournal(doc);
        }
    }

    public void close() {
        backupDocumentWriter.write(insertUnfiledNodeIntoRoot());
        backupDocumentWriter.write(excludeRedundantNodesFromUnfiledFolder());
        for (Map.Entry<Document, Document> doc : unfiledDocumentsCache.entrySet()) {
            Document fixedLastModified = fixLastModified(doc.getKey(), doc.getValue());
            backupDocumentWriter.write(updateParentForNode(fixedLastModified, unfiledFolderKey()));
        }
        try {
            journal.close();
        } catch (IOException e) {
            LOGGER.error(e, JcrI18n.problemsWritingDocumentToBackup);
        }
        unfiledDocumentsCache.clear();
        backupDocumentWriter.close();
    }

    public long getFileCount() {
        return backupDocumentWriter.getFileCount();
    }
    public long getDocumentCount(){
        return backupDocumentWriter.getDocumentCount();
    }

    public boolean containsBynaryKey(String binaryKey) {
        return journal.containsBynaryKey(binaryKey);
    }

}
