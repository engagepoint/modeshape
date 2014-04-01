package org.modeshape.jcr.brokenfolder;

import org.modeshape.jcr.JcrSession;

import javax.jcr.RepositoryException;

/**
 * @author evgeniy.shevchenko
 * @version 1.0 3/24/14
 */

public interface NodeCommand {

    NodeTaskResult execute(final JcrSession session, final String identifier)
            throws RepositoryException;


}
