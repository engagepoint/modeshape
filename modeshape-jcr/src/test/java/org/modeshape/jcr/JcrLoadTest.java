/*
 * ModeShape (http://www.modeshape.org)
 * See the COPYRIGHT.txt file distributed with this work for information
 * regarding copyright ownership.  Some portions may be licensed
 * to Red Hat, Inc. under one or more contributor license agreements.
 * See the AUTHORS.txt file in the distribution for a full listing of
 * individual contributors.
 *
 * ModeShape is free software. Unless otherwise indicated, all code in ModeShape
 * is licensed to you under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * ModeShape is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */
package org.modeshape.jcr;

import org.junit.Before;
import org.junit.Test;
import org.modeshape.common.annotation.ThreadSafe;
import org.modeshape.common.util.CheckArg;
import org.modeshape.common.util.StringUtil;
import org.modeshape.jcr.api.JcrTools;

import javax.jcr.*;
import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

public class JcrLoadTest extends SingleUseAbstractTest
{
	@Override
	@Before
	public void beforeEach() throws Exception {
		startRepositoryWithConfiguration(getClass().getClassLoader()
				.getResourceAsStream("config/repo-load-test.json"));
		tools = new JcrTools();

		// Set the transaction timeout so that we can debug code called within the transaction ...
		repository.runningState().txnManager().setTransactionTimeout(500);

		print = true;
	}

	private Binary setBinary(Node node, Binary binary) throws RepositoryException, FileNotFoundException
	{
		Node content = node.addNode("jcr:content", "nt:resource");
		content.setProperty("jcr:data", binary);

		return binary;
	}

	private class Clock
	{
		long time1, time2;

		private Clock()
		{
			time1 = System.currentTimeMillis();
		}

		public void stop()
		{
			time2 = System.currentTimeMillis();
		}

		@Override
		public String toString()
		{
			if (time2 == 0) stop();
			return "Clock: " + (time2 - time1) / 1000.0 + " sec";
		}
	}

	@Test
	public void shouldHandleRequestsPerDay() throws Exception
	{
		final Node root = session.getRootNode().addNode("/JcrLoadTest/", "nt:folder");

		// Generate 1MB of content for node
		StringBuilder builder = new StringBuilder();
		for (int i = 0; i < 1024*1024; i++) builder.append("A");

		final Binary binary = session.getValueFactory().createBinary(new ByteArrayInputStream(builder.toString().getBytes()));

		Clock clock = new Clock();

		// 100 000 / 3 (attachment 1,2 + email) ~ 33 333
		runConcurrently(33333+1, 10, new Operation() {
			@Override
			public void run( Session session, int id) throws RepositoryException, FileNotFoundException
			{
				Node attachment1 = root.addNode("attachment-one-"+id, "notifications:notificationAttachment");
				attachment1.setProperty("notifications:fileName", "file.txt");
				setBinary(attachment1, binary);

				Node attachment2 = root.addNode("attachment-two-"+id, "notifications:notificationAttachment");
				attachment2.setProperty("notifications:fileName", "file.txt");
				setBinary(attachment2, binary);

				Node email = root.addNode("email-"+id, "notifications:emailNotification");
				email.setProperty("notifications:subject", "subject");
				email.setProperty("notifications:from", "from");
				email.setProperty("notifications:to", "to");
				email.setProperty("notifications:cc", "cc");
				email.setProperty("notifications:bcc", "bcc");
				email.setProperty("notifications:attachments", new String[] {attachment1.getIdentifier(), attachment2.getIdentifier()});
				setBinary(email, binary);

				session.save();
			}
		});

		System.out.println(clock);
		System.out.println("Nodes: " + root.getNodes().getSize());
		System.out.println("Size: " + binary.getSize() / (1024*1024.0) + " MB");

//		printTree(root);
	}

	private void printTree(Node node) throws RepositoryException
	{
		if (node.getPrimaryNodeType().toString().equals("nt:resource")) return;

		System.out.println(String.format("%-50s %-50s %-50s", node.getPath(), node.getPrimaryNodeType(), node.getIdentifier()));

		NodeIterator iterator = node.getNodes();
		while(iterator.hasNext())
		{
			printTree(iterator.nextNode());
		}
	}

	/**
	 * Method that can be called within a test method to run the supplied {@link Operation} a total number of times using a
	 * specific number of threads.
	 *
	 * @param totalNumberOfOperations the total number of times the operation should be performed; must be positive
	 * @param numberOfConcurrentClients the total number of separate clients/threads that should be used; must be positive
	 * @param operation the operation to be performed
	 * @throws Exception if there is a problem executing the operation the specified number of times
	 */
	protected void runConcurrently( final int totalNumberOfOperations,
									final int numberOfConcurrentClients,
									final Operation operation ) throws Exception {
		run(totalNumberOfOperations, numberOfConcurrentClients, 0, operation);
	}

	/**
	 * An operation that can be run by clients.
	 *
	 * @see org.modeshape.jcr.JcrLoadTest#runConcurrently(int, int, Operation)
	 */
	@ThreadSafe
	protected static interface Operation {
		//void run(Session session) throws RepositoryException, Exception;
		void run(Session session, int id) throws RepositoryException, Exception;
	}

	private void run( final int totalNumberOfOperations,
					  final int numberOfConcurrentClients,
					  final int numberOfErrorsExpected,
					  final Operation operation ) throws Exception {
		CheckArg.isPositive(totalNumberOfOperations, "totalNumberOfOperations");
		CheckArg.isPositive(numberOfConcurrentClients, "numberOfConcurrentClients");
		CheckArg.isNonNegative(numberOfErrorsExpected, "numberOfErrorsExpected");

		// Create the latch ...
		final CountDownLatch startLatch = new CountDownLatch(1);
		final CountDownLatch completionLatch = new CountDownLatch(numberOfConcurrentClients);
		final Results problems = new Results();
		final AtomicInteger actualOperationCount = new AtomicInteger(1);

		// Create a session and thread for each client ...
		final Repository repository = this.repository;
		final Session[] sessions = new Session[numberOfConcurrentClients];
		Thread[] threads = new Thread[numberOfConcurrentClients];
		for (int i = 0; i != numberOfConcurrentClients; ++i) {
			sessions[i] = repository.login();
			final String threadName = "RepoClient" + (i + 1);
			Runnable runnable = new Runnable() {
				@Override
				public void run() {
					try {
						printMessage("Initializing thread '" + threadName + '"');

						// Block until all threads are ready to start ...
						startLatch.await();

						printMessage("Starting thread '" + threadName + '"');

						// Perform the operation as many times as requested ...
						int repeatCount = 1;
						while (true) {
							int operationNumber = actualOperationCount.getAndIncrement();

							if (operationNumber > totalNumberOfOperations) break;

							++repeatCount;
							Session session = null;
							printMessage("Running operation " + operationNumber + " in thread '" + threadName);
							try {
								// Create the session ...
								session = repository.login();

								// Run the operation ...
								operation.run(session, operationNumber);



							} catch (Throwable e) {
								problems.recordError(threadName, repeatCount, e);
							} finally {
								// Always log out of the session ...
								if (session != null) session.logout();

								if (operationNumber % 1000 == 0 && operationNumber > 0)
								{
									printMessage("Completed " + operationNumber + " operations");

									printMessage("Waiting 2 sec...");
									Thread.sleep(1000*2);
								}
							}
						}
					} catch (InterruptedException e) {
						Thread.interrupted();
						e.printStackTrace();
					} finally {
						// Thread is done, so count it down ...
						printMessage("Completing thread '" + threadName + '"');
						completionLatch.countDown();
					}
				}
			};
			threads[i] = new Thread(runnable, threadName);
		}

		// Start the threads ...
		for (int i = 0; i != numberOfConcurrentClients; ++i) {
			threads[i].start();
		}

		// Unlock the starting latch ...
		startLatch.countDown();

		// Wait until all threads are finished (or at most 60 seconds)...
		completionLatch.await(60, TimeUnit.SECONDS);

		// Clean up the threads ...
		for (int i = 0; i != numberOfConcurrentClients; ++i) {
			try {
				Thread thread = threads[i];
				if (thread.isAlive()) {
					thread.interrupt();
				}
			} finally {
				threads[i] = null;
			}
		}

		// Verify that we've performed the requested number of operations ...
		assertThat(actualOperationCount.get() > totalNumberOfOperations, is(true));

		// Verify there are no errors ...
		int problemsCount = problems.size();
		if (problemsCount != numberOfErrorsExpected) {
			if (numberOfConcurrentClients == 1) {
				// Just one thread, so rethrow the exception ...
				Throwable t = problems.getFirstException();
				if (t instanceof RuntimeException) {
					throw (RuntimeException)t;
				}
				if (t instanceof Error) {
					throw (Error)t;
				}
				throw (Exception)t;
			} else if (problemsCount == 0 && numberOfErrorsExpected > 0) {
				fail(numberOfErrorsExpected + " errors expected, but none occurred");
			}
			// Otherwise, multiple clients so log the set of them ...
			fail(problems.toString());
		}
	}

	protected static class Results {
		private List<Error> errors = new CopyOnWriteArrayList<Error>();

		protected void recordError( String threadName,
									int iteration,
									Throwable error ) {
			errors.add(new Error(threadName, iteration, error));
		}

		public boolean hasErrors() {
			return !errors.isEmpty();
		}

		public int size() {
			return errors.size();
		}

		public Throwable getFirstException() {
			return errors.size() > 0 ? errors.get(0).error : null;
		}

		@Override
		public String toString() {
			StringBuilder sb = new StringBuilder();
			for (Error error : errors) {
				sb.append(error.threadName)
						.append("{")
						.append(error.iteration)
						.append("} -> ")
						.append(StringUtil.getStackTrace(error.error))
						.append("\n");
			}
			return sb.toString();
		}

		protected class Error {
			protected final String threadName;
			protected final Throwable error;
			protected final int iteration;

			protected Error( String threadName,
							 int iteration,
							 Throwable error ) {
				this.threadName = threadName;
				this.iteration = iteration;
				this.error = error;
			}
		}
	}

}