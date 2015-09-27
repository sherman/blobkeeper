package io.blobkeeper.server;

/*
 * Copyright (C) 2015 by Denis M. Gabaydulin
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import com.google.inject.Guice;
import com.google.inject.Injector;
import io.blobkeeper.common.configuration.RootModule;
import org.apache.commons.daemon.Daemon;
import org.apache.commons.daemon.DaemonContext;
import org.apache.commons.daemon.DaemonInitException;
import org.slf4j.Logger;

import static org.slf4j.LoggerFactory.getLogger;

public class BlobKeeperServerApp implements Daemon {
    private static final Logger log = getLogger(BlobKeeperServerApp.class);

    private BlobKeeperServer server;

    public static void main(String[] args) throws Exception {
        BlobKeeperServerApp app = new BlobKeeperServerApp();
        app.init(null);
        app.start();
    }

    @Override
    public synchronized void init(DaemonContext daemonContext) throws DaemonInitException, Exception {
    }

    @Override
    public synchronized void start() throws Exception {
        try {
            Injector injector = Guice.createInjector(new RootModule());
            server = injector.getInstance(BlobKeeperServer.class);
            server.startAsync();
            server.awaitRunning();
        } catch (Exception e) {
            log.error("Can't start the server", e);
        }
    }

    @Override
    public synchronized void stop() throws Exception {
        try {
            this.server.stopAsync();
            this.server.awaitTerminated();
        } catch (Exception e) {
            log.error("Can't stop the server", e);
        }
    }

    @Override
    public synchronized void destroy() {
    }
}
