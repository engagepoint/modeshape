package org.modeshape.jcr;

import org.infinispan.schematic.document.Binary;
import org.infinispan.schematic.document.Document;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.util.HashSet;

import static org.modeshape.jcr.BackupDocumentWriterUtil.isDocumentNode;

/**
 * @author evgeniy.shevchenko
 * @version 1.0 4/18/2017
 */

public class BackupDocumentWriterJournal {
    private final File journal;
    private Writer writer;

    public BackupDocumentWriterJournal(File journal) {
        this.journal = journal;
    }

    void init() throws IOException {
        writer =  new FileWriter(journal);
    }

    void close() throws IOException {
        if (null != writer) {
            writer.close();
        }
    }

    void addInfoToJournal(Document doc) throws IOException {
        if (isDocumentNode(doc, Boolean.TRUE)) {
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
                        builder.append(ref);
                        builder.append(",");
                    }
                }

            }


        }
    }


}
