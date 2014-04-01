package org.modeshape.jcr.brokenfolder;

import org.modeshape.jcr.JcrRepository;

import javax.jcr.Node;
import javax.jcr.NodeIterator;
import javax.jcr.RepositoryException;
import javax.jcr.Session;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.junit.Assert.assertNotNull;

/**
 * @author evgeniy.shevchenko
 * @version 1.0 3/26/14
 */

public class UploadContentPhase extends Phase {
    private static final int POOL_SIZE = Runtime.getRuntime().availableProcessors();
    private final JcrRepository repository;
    private final List<NodeTaskResult> filesIdentifiers;

    public UploadContentPhase(
            final JcrRepository repository,
            final List<NodeTaskResult> filesIdentifiers) {
        this.repository = repository;
        this.filesIdentifiers = new ArrayList<NodeTaskResult>();
        this.filesIdentifiers.addAll(filesIdentifiers);
    }

    @Override
    protected ExecutorService createExecutorService() {
        return Executors.newFixedThreadPool(POOL_SIZE);
    }

    @Override
    protected List<NodeTask> createNodeTasks() {
        List<NodeTask> tasks = new ArrayList<NodeTask>(TASKS_COUNT);

        for (NodeTaskResult node : filesIdentifiers) {
            NodeTask task =
                    new NodeTask(
                            node.getNodeId(),
                            repository,
                            new UploadContentCommand());
            tasks.add(task);

        }
        return tasks;
    }


    @Override
    protected void validate() throws RepositoryException {
        Session session = null;
        try {
            session = repository.login();
            final Node baseFolder = session.getNode("/baseFolder");
            final NodeIterator nodeIterator = baseFolder.getNodes();
            while (nodeIterator.hasNext()) {
                Node node = nodeIterator.nextNode();
                assertNotNull(
                        "File was read without exceptions",
                        node);

            }
        } finally {
            if (session != null) {
                session.logout();
            }
        }
    }
}
