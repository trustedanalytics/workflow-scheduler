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
package org.trustedanalytics.scheduler.oozie;

import org.trustedanalytics.scheduler.oozie.jobs.sqoop.SqoopJobMapper;
import org.trustedanalytics.scheduler.client.OozieClient;
import org.trustedanalytics.scheduler.client.OozieJobId;
import org.trustedanalytics.scheduler.filesystem.OrgSpecificSpace;
import org.trustedanalytics.scheduler.filesystem.OrgSpecificSpaceFactory;
import org.trustedanalytics.scheduler.oozie.jobs.OozieScheduledJob;
import org.trustedanalytics.scheduler.oozie.jobs.sqoop.SqoopJob;
import org.trustedanalytics.scheduler.oozie.jobs.sqoop.SqoopScheduledImportJob;

import org.apache.hadoop.fs.Path;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.trustedanalytics.scheduler.oozie.serialization.CoordinatorInstance;
import org.trustedanalytics.scheduler.oozie.serialization.JobContext;
import org.trustedanalytics.scheduler.oozie.serialization.WorkflowInstance;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.UUID;
import java.util.function.Supplier;

@Service
public class OozieService {

    private static final String ERR_MSG = "Error message[${wf:errorMessage(wf:lastErrorNode())}]";
    public static final String SQOOP_DRIVER_PROPERTIES_FILE = "driver.properties";
    private final OrgSpecificSpaceFactory orgSpecificSpaceFactory;
    private final OozieClient oozieClient;
    private final Supplier<String> random;
    private SqoopJobMapper jobMapper;
    private JobContext jobContext;

    @Autowired
    public OozieService(OrgSpecificSpaceFactory orgSpecificSpaceFactory, OozieClient oozieClient, Supplier jobIdSupplier,
                          SqoopJobMapper sqoopJobMapper, JobContext jobContext) {
        this.orgSpecificSpaceFactory = orgSpecificSpaceFactory;
        this.oozieClient = oozieClient;
        this.random = jobIdSupplier;
        this.jobMapper = sqoopJobMapper;
        this.jobContext = jobContext;
    }

    public OozieJobId sqoopScheduledImportJob(SqoopScheduledImportJob job, UUID orgId) throws IOException {

        jobMapper.adjust(job);

        final OrgSpecificSpace space = orgSpecificSpaceFactory.getOrgSpecificSpace(orgId);

        final Path ooziePath = space.resolveOozieDir(job.getName(), job.getAppPath());
        final Path targetPath = space.resolveSqoopTargetDir(job.getName(), job.getSqoopImport().getTargetDir());

        job.getSqoopImport().setTargetDir("${targetDir}");

        jobContext.resolveQueueName(orgId);

        final String sqoopWf = space.createOozieWorkflow(ooziePath, sqoopWorkflow(job, new Path(ooziePath, "sqoop-create"), jobContext)).getParent().toString();

        final String sqoopCr = space.createOozieCoordinator(ooziePath, coordinator(job, sqoopWf, targetPath.toUri().toString(), jobContext)).getParent().toString();

        space.createFile(new Path(ooziePath, SQOOP_DRIVER_PROPERTIES_FILE), driverProperties(orgId.toString()) );

        return oozieClient.submitCoordinatedJob(sqoopCr);
    }

    public InputStream coordinator(OozieScheduledJob job, String path, String targetDir, JobContext jobContext) {
        return CoordinatorInstance.builder(jobContext)
                .setName(job.getName())
                .setAppPath(path)
                .setTargetDir(targetDir)
                .setOozieLibpath("/user/oozie/share/lib/")
                .setOozieUseSystemLibpath(true)
                .setOozieSchedule(job.getSchedule())
                .build()
            .asStream();
    }

    public InputStream sqoopWorkflow(SqoopScheduledImportJob sqoopImportJob,
                                     Path flagPath,
                                     JobContext jobContext) {
        final String jobId = random.get();
        final String name = sqoopImportJob.getName();
        final String sqoopExecJobName = name + "-exec";
        final String flagJobName = name + "-flag";
        final String createJobName = name + "-create";
        final String decisionNodeName = name + "-decision";

        return WorkflowInstance.builder(jobContext)
                .setName(name + "-app")
                .setStartNode(decisionNodeName)
                .sqoopFileExistDecision(flagPath.toString())
                   .setName(decisionNodeName)
                   .then(sqoopExecJobName)
                   .orElse(createJobName)
                   .and()
                .sqoopAction()
                   .setCommand(new SqoopJob(jobContext.getSqoopMetastore()).create(jobId, sqoopImportJob.getSqoopImport()).command())
                   .setName(createJobName)
                   .then(flagJobName)
                   .addFile(SQOOP_DRIVER_PROPERTIES_FILE)
                   .and()
                .createFile()
                    .setPath(flagPath.toString())
                    .setName(flagJobName)
                    .then(sqoopExecJobName)
                    .and()
                .sqoopAction()
                    .setCommand(new SqoopJob(jobContext.getSqoopMetastore()).exec(jobId, sqoopImportJob.getSqoopImport()).command())
                    .setName(sqoopExecJobName)
                    .then("end")
                    .and()
                .sqoopKill(ERR_MSG)
                .build()
                .asStream();
    }

    private InputStream driverProperties(String orgId) {
        String oracleOsuserParam = "v$session.osuser=" + orgId.substring(0,29) + "\n";
        return new ByteArrayInputStream(oracleOsuserParam.getBytes(StandardCharsets.UTF_8));
    }
}