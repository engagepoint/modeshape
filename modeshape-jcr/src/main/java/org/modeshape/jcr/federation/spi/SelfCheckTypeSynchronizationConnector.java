package org.modeshape.jcr.federation.spi;

import org.modeshape.common.collection.Problems;

/**
 * Interface that should be implemented by {@link org.modeshape.jcr.federation.spi.Connector}(s) that want to
 * implement type synchronization check
 * Created by vyacheslav.polulyakh on 5/14/2014.
 */
public interface SelfCheckTypeSynchronizationConnector {

    /**
     * Checks the synchronization between the types registered in the system and current types in a remote storage
     * @return {@link org.modeshape.common.collection.Problems} The problems encountered in type checking
     */
    Problems getSelfCheckStatus();
}
