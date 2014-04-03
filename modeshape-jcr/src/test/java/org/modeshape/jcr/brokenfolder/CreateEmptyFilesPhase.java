package org.modeshape.jcr.brokenfolder;

import org.modeshape.jcr.JcrRepository;
import org.modeshape.jcr.JcrSession;

import javax.jcr.Node;
import javax.jcr.NodeIterator;
import javax.jcr.RepositoryException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;

/**
 * @author evgeniy.shevchenko
 * @version 1.0 3/26/14
 */

public class CreateEmptyFilesPhase extends Phase {

    private final JcrRepository repository;
    private final String parentFolderId;
    private List<String> createdIds;

    public CreateEmptyFilesPhase(
            final JcrRepository repository,
            final String parentFolderId) {
        this.repository = repository;
        this.parentFolderId = parentFolderId;
    }

    @Override
    protected ExecutorService createExecutorService() {
        return Executors.newSingleThreadExecutor();
    }

    @Override
    protected List<NodeTask> createNodeTasks() {
        List<NodeTask> tasks = new ArrayList<NodeTask>(TASKS_COUNT);
        for (int i = 0; i < TASKS_COUNT; i++) {
            NodeTask task =
                    new NodeTask(
                            String.format("node-%d", i),
                            repository,
                            new CreateNodeCommand(parentFolderId));
            tasks.add(task);
        }
        return tasks;
    }


    @Override
    protected void validate() throws RepositoryException {
        JcrSession session = null;
        try {
            session = repository.login();
            final Node baseFolder = session.getNode("/baseFolder");
            final NodeIterator nodeIterator = baseFolder.getNodes();
            while (nodeIterator.hasNext()) {
                Node item = (Node) nodeIterator.next();
                assertNotNull(
                        "Child item is not null",
                        item);
            }
           /* assertThat(
                    "Folder has appropriate child nodes count",
                    nodeIterator.getSize(),
                    is((long) TASKS_COUNT));*/

        } finally {
            if (session != null) {
                session.logout();
            }
        }
    }
}
