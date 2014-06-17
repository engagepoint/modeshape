package org.modeshape.connector.cmis;

import org.modeshape.connector.cmis.api.SecondaryIdProcessor;

import java.util.UUID;
import java.util.regex.Pattern;


public class DummyCommonIdProcessor implements SecondaryIdProcessor {
    
    private static final Pattern INTERNAL_OBJECT_ID_PATTERN = Pattern.compile(".*_.{4}_.{4}_.{4}_.{12}");
    
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

    @Override
    public boolean isProcessedId(String id) {
        return INTERNAL_OBJECT_ID_PATTERN.matcher(id).matches();
    }
}
