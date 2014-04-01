package org.modeshape.jcr.brokenfolder;

import org.modeshape.jcr.JcrRepository;
import org.modeshape.jcr.JcrSession;

import javax.jcr.RepositoryException;
import java.util.concurrent.Callable;

/**
 * @version 1.0 3/24/14
 */

public class NodeTask implements Callable<NodeTaskResult> {

    private JcrRepository repository;
    private final NodeCommand nodeCommand;
    private String identifier;

    public NodeTask(
            final String identifier,
            final JcrRepository repository,
            final NodeCommand nodeCommand) {
        this.identifier = identifier;
        this.repository = repository;
        this.nodeCommand = nodeCommand;
    }

    /**
     * Computes a result, or throws an exception if unable to do so.
     *
     * @return computed result
     * @throws Exception if unable to compute a result
     */
    @Override
    public NodeTaskResult call() throws RepositoryException {
        JcrSession session = null;
        NodeTaskResult res = null;
        try {
            session = repository.login();
            res = nodeCommand.execute(session, identifier);
        } finally {
            if (session != null) {
                session.logout();
            }
        }
        return res;
    }
}