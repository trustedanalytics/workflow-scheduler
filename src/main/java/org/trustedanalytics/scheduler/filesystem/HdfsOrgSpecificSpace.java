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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.trustedanalytics.scheduler.utils.StreamUtils;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Objects;
import java.util.UUID;
import java.util.function.Supplier;

public class HdfsOrgSpecificSpace implements OrgSpecificSpace {

    private static final Logger LOGGER = LoggerFactory.getLogger(HdfsOrgSpecificSpace.class);

    private final FileSystem fileSystem;
    private final Path root;
    private final Supplier<String> random;

    public HdfsOrgSpecificSpace(FileSystem fileSystem, UUID orgId) {
        Objects.requireNonNull(fileSystem);
        Objects.requireNonNull(orgId);

        this.fileSystem = fileSystem;
        this.root = new Path(String.format("hdfs://nameservice1/org/%s/", orgId));
        this.random = () -> UUID.randomUUID().toString();
    }

    @Override
    public Path createOozieCoordinator(Path coordinatorDirPath, InputStream in) throws IOException {
        final Path coordinatorPath = new Path(coordinatorDirPath, "coordinator.xml");
        createFile(coordinatorPath, in);
        return coordinatorPath;
    }

    @Override
    public Path createOozieWorkflow(Path workflowDirPath, InputStream in) throws IOException {
        final Path workflowPath = new Path(workflowDirPath, "workflow.xml");
        createFile(workflowPath, in);
        return workflowPath;
    }

    @Override
    public Path resolveSqoopTargetDir(String jobName, String targetDir) {
        if(StringUtils.isEmpty(targetDir)) {
            return resolveDir("sqoop-imports", jobName, random.get());
        } else {
            return resolveDir(targetDir);
        }
    }

    @Override
    public Path resolveOozieDir(String jobName, String appPath) {
        if(StringUtils.isEmpty(appPath)) {
            return resolveDir("oozie-jobs", jobName, random.get());
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

    private void createFile(Path path, InputStream in) {
        try {
            StreamUtils.copy(in, fileSystem.create(path));
            LOGGER.info("Created file: " + path);
        } catch (IOException ex) {
            throw new IllegalStateException("Unable to create file: " + path, ex);
        }
    }
}


