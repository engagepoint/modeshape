package org.modeshape.jcr.federation.spi;


import org.modeshape.jcr.value.Name;

import java.util.Collection;

public interface UnfiledSupportConnector {

    Collection<Name> getApplicableUnfiledTypes();

}
