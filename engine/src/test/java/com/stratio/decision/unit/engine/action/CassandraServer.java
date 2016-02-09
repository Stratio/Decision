/**
 * Copyright (C) 2014 Stratio (http://stratio.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.stratio.decision.unit.engine.action;

import com.google.common.io.Files;
import com.google.common.io.Resources;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.commitlog.CommitLog;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.service.CassandraDaemon;
import org.apache.commons.io.Charsets;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.URL;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;

class CassandraServer {

    private static Logger log = LoggerFactory.getLogger(CassandraServer.class);

    private static final int WAIT_SECONDS = 3;

    private final String yamlFilePath;
    private CassandraDaemon cassandraDaemon;

    public CassandraServer() {
        this("/cassandra.yaml");
    }

    public CassandraServer(String yamlFile) {
        this.yamlFilePath = yamlFile;
    }

    static ExecutorService executor = Executors.newSingleThreadExecutor();

    /**
     * Set embedded cassandra up and spawn it in a new thread.
     * 
     * @throws TTransportException
     * @throws IOException
     * @throws InterruptedException
     */
    public void start() throws TTransportException, IOException, InterruptedException, ConfigurationException {

        File dir = Files.createTempDir();
        String dirPath = dir.getAbsolutePath();
        System.out.println("Storing Cassandra files in " + dirPath);

        URL url = Resources.getResource("cassandra.yaml");
        String yaml = Resources.toString(url, Charsets.UTF_8);
        yaml = yaml.replaceAll("REPLACEDIR", dirPath);
        String yamlPath = dirPath + File.separatorChar + "cassandra.yaml";
        org.apache.commons.io.FileUtils.writeStringToFile(new File(yamlPath), yaml);

        // make a tmp dir and copy cassandra.yaml and log4j.properties to it
        copy("/log4j.properties", dir.getAbsolutePath());
        System.setProperty("cassandra.config", "file:" + dirPath + yamlFilePath);
        System.setProperty("log4j.configuration", "file:" + dirPath + "/log4j.properties");
        System.setProperty("cassandra-foreground", "true");

        cleanupAndLeaveDirs();

        try {
            executor.execute(new CassandraRunner());
        } catch (RejectedExecutionException e) {
            log.error("RejectError", e);
            return;
        }

        try {
            TimeUnit.SECONDS.sleep(WAIT_SECONDS);
        } catch (InterruptedException e) {
            log.error("InterrputedError", e);
            throw new AssertionError(e);
        }
    }

    public void shutdown() throws IOException {
        executor.shutdown();
        executor.shutdownNow();
    }

    /**
     * Copies a resource from within the jar to a directory.
     * 
     * @param resource
     * @param directory
     * @throws IOException
     */
    private static void copy(String resource, String directory) throws IOException {
        mkdir(directory);
        InputStream is = CassandraServer.class.getResourceAsStream(resource);
        String fileName = resource.substring(resource.lastIndexOf("/") + 1);
        File file = new File(directory + File.separator + fileName);
        OutputStream out = new FileOutputStream(file);
        byte buf[] = new byte[1024];
        int len;
        while ((len = is.read(buf)) > 0) {
            out.write(buf, 0, len);
        }
        out.close();
        is.close();
    }

    /**
     * Creates a directory
     * 
     * @param dir
     * @throws IOException
     */
    private static void mkdir(String dir) throws IOException {
        FileUtils.createDirectory(dir);
    }

    private static void cleanupAndLeaveDirs() throws IOException {
        mkdirs();
        cleanup();
        mkdirs();
        CommitLog.instance.resetUnsafe(true); // cleanup screws w/ CommitLog, this
        // brings it back to safe state
    }

    private static void cleanup() throws IOException {

        // clean up commitlog
        String[] directoryNames = { DatabaseDescriptor.getCommitLogLocation(), };
        for (String dirName : directoryNames) {
            File dir = new File(dirName);
            if (!dir.exists()) {
                log.error("No such directory: " + dir.getAbsolutePath());
                throw new RuntimeException("No such directory: " + dir.getAbsolutePath());
            }
            FileUtils.deleteRecursive(dir);
        }

        // clean up data directory which are stored as data directory/table/data
        // files
        for (String dirName : DatabaseDescriptor.getAllDataFileLocations()) {
            File dir = new File(dirName);
            if (!dir.exists()) {
                log.error("No such directory: " + dir.getAbsolutePath());
                throw new RuntimeException("No such directory: " + dir.getAbsolutePath());
            }
            FileUtils.deleteRecursive(dir);
        }
    }

    private static void mkdirs() {
        DatabaseDescriptor.createAllDirectories();
    }

    private class CassandraRunner implements Runnable {
        @Override
        public void run() {
            cassandraDaemon = new CassandraDaemon();
            cassandraDaemon.activate();
        }
    }
}
