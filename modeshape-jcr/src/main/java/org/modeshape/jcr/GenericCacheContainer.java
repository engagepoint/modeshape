package org.modeshape.jcr;

/**
 *
 * @author leonid.marushevskiy
 */
public class GenericCacheContainer {
    private static CacheService<String, Object> instance;
    
    public static synchronized CacheService<String, Object> getInstance() {
        if (instance == null) {
            instance = new GenericCacheService<String, Object>();
        }
        return instance;
    }
    
}
