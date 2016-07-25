/**
 * Copyright (c) 2016 Intel Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.trustedanalytics.scheduler.filesystem;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.AccessControlException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.trustedanalytics.scheduler.security.TokenProvider;
import org.trustedanalytics.scheduler.utils.StreamUtils;

import org.springframework.security.access.AccessDeniedException;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Objects;
import java.util.UUID;
import java.util.function.Supplier;

public class HdfsOrgSpecificSpace implements OrgSpecificSpace {

    private static final Logger LOGGER = LoggerFactory.getLogger(HdfsOrgSpecificSpace.class);
    public static final String COORDINATOR_FILE_NAME = "coordinator.xml";
    public static final String WORKFLOW_FILE_NAME = "workflow.xml";
    public static final String SQOOP_DEFAULT_TARGET_DIR = "sqoop-imports";
    public static final String OOZIE_JOBS_DIR = "oozie-jobs";

    private final FileSystem fileSystem;
    private final Path root;
    private final Supplier<String> random;
    private final TokenProvider tokenProvider;

    public HdfsOrgSpecificSpace(FileSystem fileSystem, UUID orgId, TokenProvider tokenProvider) {
        Objects.requireNonNull(fileSystem);
        Objects.requireNonNull(orgId);

        this.fileSystem = fileSystem;
        this.root = new Path(String.format(fileSystem.getUri() + "/org/%s/", orgId));
        this.random = () -> UUID.randomUUID().toString();
        this.tokenProvider = tokenProvider;
    }

    @Override
    public Path createOozieCoordinator(Path oozieJobDir, InputStream in) throws IOException {
        final Path coordinatorPath = new Path(oozieJobDir, COORDINATOR_FILE_NAME);
        createFile(coordinatorPath, in);
        return coordinatorPath;
    }

    @Override
    public Path createOozieWorkflow(Path oozieJobDir, InputStream in) throws IOException {
        final Path workflowPath = new Path(oozieJobDir, WORKFLOW_FILE_NAME);
        createFile(workflowPath, in);
        return workflowPath;
    }

    @Override
    public Path resolveSqoopTargetDir(String jobName, String targetDir) {

        if(StringUtils.isEmpty(targetDir)) {
            return resolveDir(SQOOP_DEFAULT_TARGET_DIR, jobName, random.get());
        } else {
            return resolveDir("user", tokenProvider.getUserId(), targetDir);
        }
    }

    @Override
    public Path resolveOozieDir(String jobName, String appPath) {
        if(StringUtils.isEmpty(appPath)) {
            return resolveDir(OOZIE_JOBS_DIR, jobName, random.get());
        } else {
            return resolveDir(appPath);
        }
    }

    @Override
    public String getNameNode() {
        return fileSystem.getUri().toString();
    }

    private Path resolveDir(String path, String... more) {
        return Arrays.asList(more)
                     .stream()
                     .reduce(new Path(root, path), Path::new, Path::new);
    }

    @Override
    public void createFile(Path path, InputStream in) {
        try {
            StreamUtils.copy(in, fileSystem.create(path));
            LOGGER.info("Created file: " + path);
        } catch (AccessControlException ex) {
            throw new AccessDeniedException("Permission denied for given organization", ex);
        } catch (IOException ex) {
            throw new IllegalStateException("Unable to create file: " + path, ex);
        }
    }
}


