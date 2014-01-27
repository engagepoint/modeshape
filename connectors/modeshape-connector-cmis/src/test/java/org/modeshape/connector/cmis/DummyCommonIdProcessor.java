package org.modeshape.connector.cmis;

import org.apache.poi.hssf.record.HyperlinkRecord;
import org.modeshape.connector.cmis.api.SecondaryIdProcessor;

import java.util.UUID;


public class DummyCommonIdProcessor implements SecondaryIdProcessor {
    @Override
    public String preProcessIdValue(String id) {
        try {
            UUID uuid = UUID.fromString(id);
            return id.replace("-","_");
        } catch (IllegalArgumentException iae) {
            return id;
        }
    }

    public static void main(String[] args) {
        UUID.fromString("c7ae24c3-0915-499d-8b4d-4160a31cb3fb");
        UUID.fromString("idd_c7ae24c3-0915-499d-8b4d-4160a31cb3fb");
    }
}
