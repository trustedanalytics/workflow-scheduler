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

import org.trustedanalytics.scheduler.OozieJobMapper;
import org.trustedanalytics.scheduler.OozieJobValidator;
import org.trustedanalytics.scheduler.client.OozieClient;
import org.trustedanalytics.scheduler.client.OozieJobId;
import org.trustedanalytics.scheduler.filesystem.OrgSpecificSpace;
import org.trustedanalytics.scheduler.filesystem.OrgSpecificSpaceFactory;
import org.trustedanalytics.scheduler.oozie.jobs.OozieScheduledJob;
import org.trustedanalytics.scheduler.oozie.jobs.sqoop.SqoopImport;
import org.trustedanalytics.scheduler.oozie.jobs.sqoop.SqoopJob;
import org.trustedanalytics.scheduler.oozie.jobs.sqoop.SqoopScheduledImportJob;
import org.trustedanalytics.scheduler.oozie.serialization.*;

import com.google.common.collect.Lists;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.Path;
import org.simpleframework.xml.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.trustedanalytics.scheduler.utils.OozieNameResolver;

import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.UUID;
import java.util.function.Supplier;

@Service
public class OozieService {

    private static final Logger LOGGER = LoggerFactory.getLogger(OozieService.class);
    private static final String ERR_MSG = "Error message[${wf:errorMessage(wf:lastErrorNode())}]";
    private final OrgSpecificSpaceFactory orgSpecificSpaceFactory;
    private final OozieClient oozieClient;
    private final Serializer serializer;
    private final Supplier<String> random;
    private OozieJobValidator jobValidator;
    private OozieJobMapper jobMapper;

    @Value("${job.tracker}")
    private String jobTracker;

    @Value("${sqoop.metastore}")
    private String sqoopMetastore;

    @Autowired
    public OozieService(OrgSpecificSpaceFactory orgSpecificSpaceFactory, OozieClient oozieClient,
                        Serializer serializer, Supplier jobIdSupplier, OozieJobValidator oozieJobValidator, OozieJobMapper oozieJobMapper) {
        this.orgSpecificSpaceFactory = orgSpecificSpaceFactory;
        this.oozieClient = oozieClient;
        this.serializer = serializer;
        this.random = jobIdSupplier;
        this.jobValidator = oozieJobValidator;
        this.jobMapper = oozieJobMapper;
    }

    public OozieJobId sqoopScheduledImportJob(SqoopScheduledImportJob job, UUID orgId)
        throws IOException {

        jobValidator.validate(job);
        jobMapper.adjust(job);

        final OrgSpecificSpace space = orgSpecificSpaceFactory.getOrgSpecificSpace(orgId);
        final SqoopImport sqoopImport = job.getSqoopImport();

        final Path ooziePath = space.resolveOozieDir(job.getName(), job.getAppPath());
        final Path targetPath = space.resolveSqoopTargetDir(job.getName(), sqoopImport.getTargetDir());
        final Path flagPath = new Path(ooziePath, "sqoop-create");

        job.getSqoopImport().setTargetDir("${targetDir}");

        final String jobId = random.get();
        final SqoopJob createJob = new SqoopJob(sqoopMetastore).create(jobId, sqoopImport);
        final SqoopJob execJob = new SqoopJob(sqoopMetastore).exec(jobId, sqoopImport);

        final String nameNode = space.getNameNode();

        final String sqoopWf = space.createOozieWorkflow(ooziePath, sqoopWorkflow(job, createJob, execJob, flagPath, nameNode, orgId.toString())).getParent().toString();
        final String sqoopCr = space.createOozieCoordinator(ooziePath, coordinator(job, sqoopWf, targetPath.toUri().toString(), nameNode, orgId.toString())).getParent().toString();

        return oozieClient.submitCoordinatedJob(sqoopCr);
    }


    public InputStream coordinator(OozieScheduledJob job, String path, String targetDir, String nameNode, String queueName) {

        final List<PropertyNode> configuration =
            Lists.newArrayList(new PropertyNode("jobTracker", jobTracker),
                               new PropertyNode("nameNode", nameNode),
                               new PropertyNode("queueName", queueName),
                               new PropertyNode("targetDir", targetDir),
                               new PropertyNode("oozie.libpath", "/user/oozie/share/lib/"),
                               new PropertyNode("oozie.use.system.libpath", "true"));

        final OozieSchedule schedule = job.getSchedule();

        final CoordinatorWorkflowNode workflow = new CoordinatorWorkflowNode();
        workflow.setAppPath(path);
        workflow.setConfiguration(configuration);

        final CoordinatorActionNode action = new CoordinatorActionNode();
        action.setWorkflow(workflow);

        final CoordinatorAppNode coordinator = new CoordinatorAppNode();
        coordinator.setName(job.getName());
        coordinator.setFrequency(schedule.coordinatorFrequency());
        coordinator.setStart(schedule.coordinatorStart());
        coordinator.setEnd(schedule.coordinatorEnd());
        coordinator.setTimezone(schedule.coordinatorZone());
        coordinator.setAction(action);

        return toInputStream(coordinator);
    }

    public InputStream sqoopWorkflow(SqoopScheduledImportJob sqoopImportJob,
        SqoopJob sqoopCreateJob, SqoopJob sqoopExecJob, Path flagPath, String nameNode, String queueName) {

        final String workflowName = OozieNameResolver.resolveWorkflowAppName(sqoopImportJob.getName());
        final String decisionName = sqoopImportJob.getName() + "-decision";
        final String createName = sqoopImportJob.getName() + "-create";
        final String flagName = sqoopImportJob.getName() + "-flag";
        final String execName = sqoopImportJob.getName() + "-exec";
        final String endName = "end";
        final String failName = "fail";

        final WorkflowAppNode workflowApp = new WorkflowAppNode(workflowName);
        workflowApp.setStart(new StartNode(decisionName));
        workflowApp.setEnd(new EndNode(endName));
        workflowApp.setKill(new KillNode(failName, ERR_MSG));

        final DecisionNode decision = new DecisionNode(decisionName);
        final List<SwitchCase> cases = Lists.newArrayList(
            new CaseNode(createName, String.format("${fs:exists(\"%s\") eq false}", flagPath.toString())),
            new DefaultNode(execName)
        );
        decision.setCases(cases);
        workflowApp.setDecision(decision);

        final List<PropertyNode> configuration =
            Lists.newArrayList(new PropertyNode("mapred.job.queue.name", queueName));

        final List<FsAction> prepare = Lists.newArrayList();
        if (sqoopImportJob.getSqoopImport().getOverwrite()) {
            prepare.add(new DeleteNode(sqoopImportJob.getSqoopImport().getTargetDir()));
            prepare.add(new MkdirNode(sqoopImportJob.getSqoopImport().getTargetDir()));
        }

        final List<WorkflowActionNode> workflowActions = Lists.newArrayList();

        final WorkflowActionNode workflowCreateSqoop = new WorkflowActionNode(createName);
        workflowCreateSqoop.setOk(new OkNode(flagName));
        workflowCreateSqoop.setError(new ErrorNode(failName));

        final SqoopActionNode sqoopCreate = new SqoopActionNode();
        sqoopCreate.setCommand(sqoopCreateJob.command());
        sqoopCreate.setJobTracker(jobTracker);
        sqoopCreate.setNameNode(nameNode);
        sqoopCreate.setPrepare(Lists.newArrayList());
        sqoopCreate.setConfiguration(configuration);

        workflowCreateSqoop.setAction(sqoopCreate);
        workflowActions.add(workflowCreateSqoop);

        final WorkflowActionNode workflowCreateFlag = new WorkflowActionNode(flagName);
        workflowCreateFlag.setOk(new OkNode(execName));
        workflowCreateFlag.setError(new ErrorNode(failName));

        final FsActionNode fsTouch = new FsActionNode();
        fsTouch.setPrepare(Lists.newArrayList(new TouchNode(flagPath.toString())));
        workflowCreateFlag.setAction(fsTouch);
        workflowActions.add(workflowCreateFlag);

        final WorkflowActionNode workflowExecSqoop = new WorkflowActionNode(execName);
        workflowExecSqoop.setOk(new OkNode(endName));
        workflowExecSqoop.setError(new ErrorNode(failName));

        final SqoopActionNode sqoopExec = new SqoopActionNode();
        sqoopExec.setCommand(sqoopExecJob.command());
        sqoopExec.setJobTracker(jobTracker);
        sqoopExec.setNameNode(nameNode);
        sqoopExec.setConfiguration(configuration);
        sqoopExec.setPrepare(prepare);
        workflowExecSqoop.setAction(sqoopExec);
        workflowActions.add(workflowExecSqoop);

        workflowApp.setWorkflowActions(workflowActions);

        return toInputStream(workflowApp);
    }

    // TODO Write input stream implementation to intercept incoming lines
    private InputStream toInputStream(Object object) {
        try (StringWriter writer = new StringWriter(4 * 1024)) {
            serializer.write(object, writer);

            String configuration = writer.toString();
            LOGGER.info("\n{}", configuration);

            return IOUtils.toInputStream(configuration, StandardCharsets.UTF_8);
        } catch (Exception ex) {
            throw new IllegalStateException("Unable to convert object to input stream", ex);
        }
    }

}
