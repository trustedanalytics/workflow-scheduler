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
package org.trustedanalytics.scheduler.utils;

import lombok.Getter;
import lombok.Setter;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.Path;
import org.trustedanalytics.scheduler.filesystem.OrgSpecificSpace;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

// no thread safe for unit tests only
public class InMemoryOrgSpecificSpace implements OrgSpecificSpace {

    @Getter
    private static String coordinatorXml;
    @Getter
    private static String workflowXml;

    @Setter
    private static String sqoopTargetDir = "SqoopTargetDirectory";
    @Setter
    private static String oozieDirectory = "oozieDirectory";

    private static Map<Path, String> files = new HashMap<>();


    @Override
    public Path createOozieCoordinator(Path coordinatorDirPath, InputStream in) throws IOException {
        coordinatorXml = IOUtils.toString(in, "UTF-8");
        return coordinatorDirPath;
    }

    @Override
    public Path createOozieWorkflow(Path workflowDirPath, InputStream in) throws IOException {
        workflowXml = IOUtils.toString(in, "UTF-8");
        return new Path(workflowDirPath,"workflow.xml");
    }

    @Override
    public Path resolveSqoopTargetDir(String jobName, String targetDir) {
        return new Path(sqoopTargetDir);
    }

    @Override
    public Path resolveOozieDir(String jobName, String appPath) {
        if(StringUtils.isEmpty(appPath)) {
            return resolveDir("oozie-jobs", jobName, "FAKE_GUID");
        } else {
            return resolveDir(appPath);
        }
    }

    @Override
    public String getNameNode() {
        return "test_namenode";
    }

    @Override
    public void createFile(Path path, InputStream in) {
        try {
            files.put(path,IOUtils.toString(in));
        } catch (IOException e) {
            throw new IllegalStateException(String.format("Could not create file in path: (%s)", path));
        }
    }

    private Path resolveDir(String path, String... more) {
        return Arrays.asList(more)
                .stream()
                .reduce(new Path(path), Path::new, Path::new);
    }
}