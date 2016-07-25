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

import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.io.InputStream;

public interface OrgSpecificSpace {
    Path createOozieCoordinator(Path coordinatorDirPath, InputStream in) throws IOException;

    Path createOozieWorkflow(Path workflowDirPath, InputStream in) throws IOException;

    Path resolveSqoopTargetDir(String jobName, String targetDir);

    Path resolveOozieDir(String jobName, String appPath);

    String getNameNode();

    void createFile(Path path, InputStream in);
}
