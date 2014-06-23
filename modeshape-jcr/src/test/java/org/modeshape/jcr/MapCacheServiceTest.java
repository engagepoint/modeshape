package org.modeshape.jcr;

import org.junit.Assert;
import org.junit.Test;

/**
 *
 * @author leonid.marushevskiy
 */
public class MapCacheServiceTest {
    
    private static final int MAX_CACHE_SIZE = 100;
    private static final int INITIAL_CACHE_SIZE = 10;
    
    private CacheService<String, Object> instance = new MapCacheService<String, Object>(INITIAL_CACHE_SIZE, MAX_CACHE_SIZE);     
    
    
    @Test
    public void shouldBeLimitedBySize() {
        for (int i = 0; i < MAX_CACHE_SIZE*2; i++) {
            String value = "Some value";
            instance.put("key"+i, value);
            Assert.assertTrue("Cache must be limited by max size", instance.size() <= MAX_CACHE_SIZE);                        
        }        
    }
    
    @Test
    public void shouldReturnValueByKey() {
        for (int i = 0; i < MAX_CACHE_SIZE*2; i++) {
            String value = "Some value";
            instance.put("key"+i, value);            
        }                
        Assert.assertNull("Return null if element not exists in cache", instance.get("key1"));
        Assert.assertEquals("Return valid value for existing element", "Some value", instance.get("key150"));
    }
    
    @Test
    public void shouldReturnActualSize() {
        // For items count less than max_cache_size
        for (int i = 0; i < MAX_CACHE_SIZE; i++) {
            String value = "Some value";
            instance.put("key"+i, value);       
            Assert.assertTrue("Cache must return actual size", instance.size() == (i+1));
        }
        // And when cache is full
        for (int i = 0; i < MAX_CACHE_SIZE*2; i++) {
            String value = "Some value";
            instance.put("key"+i, value);       
            Assert.assertTrue("Cache must return actual size", instance.size() == MAX_CACHE_SIZE);
        }
    }
    
    @Test
    public void shouldWorkFastForBigCapacity() {
        CacheService<String, Object> bigCache = new MapCacheService<String, Object>(INITIAL_CACHE_SIZE, 100000);     
        for (int i = 0; i < 1000000; i++) {
            String value = "Some value";
            bigCache.put("key"+i, value);  
            Assert.assertTrue("Cache must return actual size", bigCache.size() <= 100000);        
        }        
        Assert.assertNull("Return null if element not exists in cache", bigCache.get("key1"));
        Assert.assertEquals("Return valid value for existing element", "Some value", bigCache.get("key999999"));        
    }
    
}