/*
 * ModeShape (http://www.modeshape.org)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.modeshape.jcr.benchmark;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;
import org.modeshape.jcr.ClusteringHelper;
import org.modeshape.jcr.JcrRepository;
import org.modeshape.jcr.TestingUtil;

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.Executors;

import javax.jcr.RepositoryException;

import com.carrotsearch.junitbenchmarks.BenchmarkRule;

/**
 * Execute test in cluster environment.
 *
 *
 * @author evgeniy.shevchenko
 * @version 1.0 4/14/14
 */
public abstract class AbstractBenchmarkTest {
    protected static int DEFAULT_POOL_SIZE = Runtime.getRuntime().availableProcessors();




    /**
     * Count of cluster nodes.
     */
    protected static int CLUSTER_NODES_COUNT = 4;

    /**
     * Array of cluster repositories.
     */
    protected JcrRepository[] repositories;

    protected String methodName;
    /**
     *  A benchmark rule to  execution time of test action.
     */
    @Rule
    public TestRule rule = RuleChain.outerRule(
            new TestWatcher() {
                /**
                 * Start all repositories and run the test action.
                 */
                @Override
                public Statement apply(final Statement base, final Description description) {
                   return new Statement() {
                        @Override
                        public void evaluate() throws Throwable {
                            // Start repositories before test will be executed to exclude
                            // "cluster startup time" from measure.
                            startRepositories(description);
                            base.evaluate();
                        }
                    };
                }


                /**
                 * Start all cluster nodes.
                 * @throws Exception
                 */
                private void startRepositories(final Description description)
                        throws Exception {
                    methodName = description.getMethodName();
                    System.setProperty("cluster.testname", methodName);
                    repositories = new JcrRepository[CLUSTER_NODES_COUNT];
                    for (int i = 0; i < CLUSTER_NODES_COUNT; i++) {
                        System.setProperty(
                                "jgroups.tcp.port",
                                Integer.toString(17900 + i));
                        System.setProperty(
                                "cluster.item.number",
                                Integer.toString(i));

                        repositories[i] =
                                TestingUtil
                                        .startRepositoryWithConfig(
                                                String.format(
                                                        "cluster/%s/repo.json", methodName));
                    }
                }

            }).around(new BenchmarkRule());

    /**
     * Bind Jgroups, init system variables.
     * @throws Exception
     */
    @BeforeClass
    public static void beforeClass() throws Exception {

        ClusteringHelper.bindJGroupsToLocalAddress();
    }

    /**
     * Unbind Jgroups.
     * @throws Exception
     */
    @AfterClass
    public static void afterClass() throws Exception {
        ClusteringHelper.removeJGroupsBindings();
    }

    /**
     * Stop engines.
     * @throws Exception
     */
    @After
    public void after() throws Exception {
        Thread.sleep(3000);
        validate();
        TestingUtil.killRepositories(repositories);
    }

    /**
     * Execute {@see MoveNodeTask} tasks.
     * @throws Exception
     */
    protected void executeTest() throws Exception {
        executeTest(DEFAULT_POOL_SIZE);
    }
    /**
     * Execute {@see MoveNodeTask} tasks.
     * @throws Exception
     */
    @SuppressWarnings("unchecked")
    protected void executeTest(final int poolSize)
                throws Exception {
        CompletionService<String> completionService =
                new ExecutorCompletionService(
                        Executors.newFixedThreadPool(poolSize));
        List<Callable<String>> tasks = generateTasks();
        for (Callable task : tasks) {
            completionService.submit(task);
        }
        for (int i = 0; i < tasks.size(); i++) {
            String taskResult =
                    completionService.take().get();
            processTaskResult(taskResult);
        }
    }




    protected abstract List<Callable<String>> generateTasks() throws RepositoryException;
    protected abstract void validate() throws Exception;
    protected abstract void processTaskResult(String result) throws Exception;
}
