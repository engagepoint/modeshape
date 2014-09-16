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
package org.modeshape.jcr.value.binary;

import de.flapdoodle.embed.mongo.Command;
import de.flapdoodle.embed.mongo.MongodExecutable;
import de.flapdoodle.embed.mongo.MongodProcess;
import de.flapdoodle.embed.mongo.MongodStarter;
import de.flapdoodle.embed.mongo.config.*;
import de.flapdoodle.embed.mongo.distribution.Version;
import de.flapdoodle.embed.process.config.IRuntimeConfig;
import de.flapdoodle.embed.process.config.store.HttpProxyFactory;
import de.flapdoodle.embed.process.config.store.IProxyFactory;
import de.flapdoodle.embed.process.config.store.NoProxyFactory;
import de.flapdoodle.embed.process.io.progress.LoggingProgressListener;
import de.flapdoodle.embed.process.runtime.Network;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import java.io.IOException;
import java.util.UUID;
import java.util.logging.FileHandler;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Setup mongodb env and uncomment tests to run.
 * 
 * @author kulikov
 * @author Horia Chiorean (hchiorea@redhat.com)
 */
public class MongodbBinaryStoreTest extends AbstractBinaryStoreTest {
    private static final Logger LOGGER = Logger.getLogger("MongoDBOutput");

    private static MongodProcess mongodProcess;
    private static MongodExecutable mongodExecutable;

    private static MongodbBinaryStore binaryStore;

    static {
        try {
            LOGGER.addHandler(new FileHandler("target/mongoDB_output.txt", false));
            LOGGER.setLevel(Level.SEVERE);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @BeforeClass
    public static void setUpClass() throws Exception {

        boolean proxyEnabled = Boolean.valueOf(System.getProperty("proxyEnabled", "false"));

        Command command = Command.MongoD;

        IProxyFactory proxy;

        if (proxyEnabled) {
            String proxyHost = System.getProperty("proxyHost");
            int proxyPort = Integer.valueOf(System.getProperty("proxyPort"));
            proxy = new HttpProxyFactory(proxyHost, proxyPort);
        } else {
            proxy = new NoProxyFactory();
        }

        IRuntimeConfig runtimeConfig = new RuntimeConfigBuilder()
                .defaults(command)
                .processOutput(MongodProcessOutputConfig.getInstance(command, LOGGER))
                .artifactStore(new ArtifactStoreBuilder()
                        .defaults(command)
                        .download(new DownloadConfigBuilder()
                                .defaultsForCommand(command)
                                .progressListener(new LoggingProgressListener(LOGGER, Level.FINE))
                                .proxyFactory(proxy)))
                .build();


        MongodStarter runtime = MongodStarter.getInstance(runtimeConfig);
        int freeServerPort = Network.getFreeServerPort();

        IMongodConfig mongodConfig = new MongodConfigBuilder()
                .version(Version.Main.PRODUCTION)
                .net(new Net(freeServerPort, false))
                .build();

        mongodExecutable = runtime.prepare(mongodConfig);
        mongodProcess = mongodExecutable.start();

        binaryStore = new MongodbBinaryStore("localhost", freeServerPort, "test-" + UUID.randomUUID());
        binaryStore.start();
    }

    @AfterClass
    public static void tearDownClass() throws Exception {
        try {
            binaryStore.shutdown();
            mongodExecutable.stop();
            mongodProcess.stop();
        } catch (Throwable t) {
            //ignore
        }
    }

    @Override
    protected BinaryStore getBinaryStore() {
        return binaryStore;
    }
}
