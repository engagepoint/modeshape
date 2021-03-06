package org.modeshape.jcr.benchmark;

import com.carrotsearch.junitbenchmarks.BenchmarkOptions;
import com.carrotsearch.junitbenchmarks.annotation.BenchmarkMethodChart;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.modeshape.jcr.JcrRepository;
import org.modeshape.jcr.JcrSession;

import javax.jcr.Node;
import java.io.FileInputStream;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;

/**
 * Test to measure execution time for creating documents with different configurations in cluster.
 * Report will be generated to <a href="../modeshape-jcr/target/benchmark-create-documents/report.html">file</a>
 * @author evgeniy.shevchenko
 * @version 1.0 4/29/14
 */
@BenchmarkMethodChart(filePrefix = "../modeshape-jcr/target/benchmark-create-documents/report")
public class CreateDocumentsBenchmarkTest extends AbstractBenchmarkTest {


    /**
     * Count of documents to create.
     */
    private static int TASKS_COUNT = 100;

    private static final String BODY = "../modeshape-jcr/src/test/resources/cluster/%s/repo.json";
    /**
     * List of documents identifiers, which will be validated on all
     * cluster nodes.
     */
    private Set<String> taskResults =
            new HashSet<String>(TASKS_COUNT);

    @Before
    public void before() {
        taskResults.clear();
    }
    /**
     * Test async distribution mode with tcp transport
     * Full configuration can be found by this
     * <a href="file:////src/test/resources/cluster/createDocumentsAsyncTcp/repo.json">link</a>
     * @throws Exception on error
     */
    //unsupported configuration
    @Ignore
    @Test
    @BenchmarkOptions(benchmarkRounds = 1, warmupRounds = 0)
    public void createDocumentsAsyncTcp() throws Exception{
        executeTest();
    }

    /**
     * Test async replication mode with tcp transport
     * Full configuration can be found by this
     * <a href="file:////src/test/resources/cluster/createDocumentsSyncTcp/repo.json">link</a>
     * @throws Exception on error
     */
    @Test
    @BenchmarkOptions(benchmarkRounds = 1, warmupRounds = 0)
    public void createDocumentsSyncTcp() throws Exception{
        executeTest();
    }

    /**
     * Generate list of {@see Callable} tasks.
     * One task will login to random repo in cluster and create document
     * with body.
     * @return List of tasks
     */
    @Override
    protected List<Callable<String>> generateTasks() {
        return BenchmarkTestHelper
                .generateCreateDocumentsTasks(
                        repositories, TASKS_COUNT, methodName);
    }


    /**
     * Validate that all created objects are available in all repositories
     * after replication.
     * @throws Exception on error
     */
    @Override
    protected void validate() throws Exception {
        for (JcrRepository repository: repositories) {
            JcrSession session = null;
            try {
                session = repository.login();
                BenchmarkTestHelper
                        .validateRepository(
                                session,
                                taskResults,
                                "/",
                                "ClusteredRepository",
                                TASKS_COUNT,
                                false);
                System.out.println("Total items count " + ((Node)session.getRootNode()).getNodes().getSize());
            } finally {
                if (session != null) {
                    session.logout();
                }
            }
        }
    }

    @Override
    protected void processTaskResult(String result) throws Exception {
        taskResults.add(result);
    }

    /**
     * {@see Callable} task for creating documents in root folder.
     * @author evgeniy.shevchenko
     * @version 1.0 3/24/14
     */

     static class CreateDocumentTask implements Callable<String> {

        private JcrRepository repository;
        private String methodName;
        public CreateDocumentTask(
                final JcrRepository repository,
                final String methodName) {
            this.repository = repository;
            this.methodName = methodName;
        }

        /**
         * Create document in root folder.
         *
         * @return document identifier
         * @throws Exception if unable to compute a result
         */
        @Override
        public String call() throws Exception {
            JcrSession session = null;

            try {
                session = repository.login();
                Node root = session.getRootNode();

                Node file = root.addNode(
                        UUID.randomUUID().toString(), "nt:file");
                Node resource = file.addNode("jcr:content", "nt:resource");
                resource.setProperty(
                        "jcr:data",
                        session.getValueFactory()
                                .createBinary(
                                        new FileInputStream(
                                                String.format(BODY, methodName))));
                session.save();
                return file.getIdentifier();
            } finally {
                if (session != null) {
                    session.logout();
                }
            }

        }
    }


}
