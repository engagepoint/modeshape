package org.modeshape.jcr.federation.spi;

import org.modeshape.jcr.value.Name;


public interface EnhancedConnector{

    int getChildCount(String parentKey, Name name);

}
