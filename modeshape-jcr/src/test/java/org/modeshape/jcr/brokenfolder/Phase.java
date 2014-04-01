package org.modeshape.jcr.brokenfolder;

import javax.jcr.RepositoryException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

/**
 * @author evgeniy.shevchenko
 * @version 1.0 3/26/14
 */

public abstract class Phase {
    public static final int TASKS_COUNT = 100;
    private List<NodeTaskResult> taskResults =
            new ArrayList<NodeTaskResult>(TASKS_COUNT);


    private List<Future<NodeTaskResult>> submitTasks(
            final ExecutorService executorService) {
        List<NodeTask> tasks = createNodeTasks();
        List<Future<NodeTaskResult>> results =
                new ArrayList<Future<NodeTaskResult>>();
        for (NodeTask task : tasks) {
            results.add(executorService.submit(task));
        }
        return results;
    }


    public void execute()
            throws RepositoryException, ExecutionException, InterruptedException {

        ExecutorService executorService =
                createExecutorService();
        try {
            List<Future<NodeTaskResult>> results = submitTasks(executorService);
            taskResults.clear();
            for (Future<NodeTaskResult> future : results) {
                NodeTaskResult taskResult = future.get();
                proccessResult(taskResult);
            }
            validate();

        } finally {
            executorService.shutdownNow();
        }
    }

    protected void proccessResult(NodeTaskResult result) {
        taskResults.add(result);
      /*  System.out.println(
                String.format(
                        "Node{name='%s', id='%s', contentId='%s'}",
                        result.getNodeName(),
                        result.getNodeId(),
                        result.getNodeContentId()));*/
    }

    public List<NodeTaskResult> getTaskResults() {
        return taskResults;
    }

    protected abstract ExecutorService createExecutorService();

    protected abstract List<NodeTask> createNodeTasks();

    protected abstract void validate() throws RepositoryException;
}
