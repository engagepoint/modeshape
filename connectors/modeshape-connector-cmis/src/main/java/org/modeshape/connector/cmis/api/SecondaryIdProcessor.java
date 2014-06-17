package org.modeshape.connector.cmis.api;

/*
 * secondary processing tool
 * defined in the configuration per system type
 */
public interface SecondaryIdProcessor {

    /*
    * preProcess value before persisting or
    * using in the search query
    */
    String preProcessIdValue(String id);
    
    boolean isProcessedId(String id);

}
