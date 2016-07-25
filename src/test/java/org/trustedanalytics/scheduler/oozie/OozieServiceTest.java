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

import org.apache.commons.lang.StringUtils;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.MockitoAnnotations;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.Environment;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.trustedanalytics.scheduler.oozie.jobs.sqoop.SqoopImport;
import org.trustedanalytics.scheduler.oozie.jobs.sqoop.SqoopScheduledImportJob;
import org.trustedanalytics.scheduler.utils.FileLoader;
import org.trustedanalytics.scheduler.utils.InMemoryOrgSpecificSpace;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.UUID;

import static junit.framework.TestCase.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes=TestConfiguration.class)
public class OozieServiceTest  {

    @Autowired
    private Environment env;

    @Autowired
    OozieService oozieService;

    @Before
    public void prepare() {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void createValidXMLConfigTestAppendModeUTC() throws IOException {

        SqoopScheduledImportJob sqoopScheduledImportJob = new SqoopScheduledImportJob();
        sqoopScheduledImportJob.setName("test");
        OozieSchedule oozieSchedule = new OozieSchedule(LocalDateTime.of(2077,7,4,8,15),
                                                    LocalDateTime.of(2077,7,6,8,15), ZoneId.of("UTC"));
        OozieFrequency frequency = new OozieFrequency();
        frequency.setAmount(10L);
        frequency.setUnit("minutes");

        oozieSchedule.setFrequency(frequency);

        sqoopScheduledImportJob.setSchedule(oozieSchedule);

        SqoopImport sqoopImport = new SqoopImport();
        sqoopImport.setJdbcUri("FAKE_JDBC_URI");
        sqoopImport.setTable("table_in_database");
        sqoopImport.setImportMode("append");
        sqoopImport.setUsername("john");
        sqoopImport.setPassword("doe");
        sqoopScheduledImportJob.setSqoopImport(sqoopImport);
        UUID orgId = UUID.fromString("1981838e-bcc9-4402-95eb-60c7f3ca6fbc");

        oozieService.sqoopScheduledImportJob(sqoopScheduledImportJob, orgId);

        String generatedWorkflow = InMemoryOrgSpecificSpace.getWorkflowXml().replaceAll("[ \t\r]","").trim();
        String generatedCoordinator = InMemoryOrgSpecificSpace.getCoordinatorXml().replaceAll("[ \t\r]", "").trim();

        String validWorkflow = FileLoader.readFileResourceNormalized("/workflow.xml");
        String validCoordinator = FileLoader.readFileResourceNormalized("/coordinator.xml");

        String workflowDiff = StringUtils.difference(generatedWorkflow.trim(), validWorkflow.trim());
        String coordinatorDiff = StringUtils.difference(generatedCoordinator.trim(),validCoordinator.trim());

        System.out.println("Workflow difference: " + workflowDiff);
        System.out.println("Coordinator difference: " + coordinatorDiff);

        assertTrue(workflowDiff.length() == 0);
        assertTrue(coordinatorDiff.length() == 0);
    }



    @Test
    public void createValidXMLConfigTestAppendModeLosAngelesTime() throws IOException {
        String timeZone = "America/Los_Angeles";
        SqoopScheduledImportJob sqoopScheduledImportJob = new SqoopScheduledImportJob();
        sqoopScheduledImportJob.setName("test");
        OozieSchedule oozieSchedule = new OozieSchedule(LocalDateTime.of(2077,7,4,1,15),
                LocalDateTime.of(2077,7,6,1,15), ZoneId.of(timeZone));
        OozieFrequency frequency = new OozieFrequency();
        frequency.setAmount(10L);
        frequency.setUnit("minutes");

        oozieSchedule.setFrequency(frequency);
        sqoopScheduledImportJob.setSchedule(oozieSchedule);

        SqoopImport sqoopImport = new SqoopImport();
        sqoopImport.setJdbcUri("FAKE_JDBC_URI");
        sqoopImport.setTable("table_in_database");
        sqoopImport.setImportMode("append");
        sqoopImport.setUsername("john");
        sqoopImport.setPassword("doe");
        sqoopScheduledImportJob.setSqoopImport(sqoopImport);
        UUID orgId = UUID.fromString("1981838e-bcc9-4402-95eb-60c7f3ca6fbc");


        oozieService.sqoopScheduledImportJob(sqoopScheduledImportJob, orgId);

        String generatedWorkflow = InMemoryOrgSpecificSpace.getWorkflowXml().replaceAll("[ \t\r]", "").trim();;
        String generatedCoordinator = InMemoryOrgSpecificSpace.getCoordinatorXml().replaceAll("[ \t\r]", "").trim();

        String validWorkflow = FileLoader.readFileResourceNormalized("/workflow.xml");
        String validCoordinator = FileLoader.readFileResourceNormalized("/coordinator.xml");

        validCoordinator = validCoordinator.replace("UTC",timeZone);

        String workflowDiff = StringUtils.difference(generatedWorkflow.trim(), validWorkflow.trim());
        String coordinatorDiff = StringUtils.difference(generatedCoordinator.trim(),validCoordinator.trim());

        System.out.println("Workflow difference: " + workflowDiff);
        System.out.println("Coordinator difference: " + coordinatorDiff);

        assertTrue(workflowDiff.length() == 0);
        assertTrue(coordinatorDiff.length() == 0);
    }

    @Test
    public void importModeIncrementalFlagsAreSetCorrectly() throws IOException {
        SqoopScheduledImportJob sqoopScheduledImportJob = new SqoopScheduledImportJob();
        sqoopScheduledImportJob.setName("test");
        OozieSchedule oozieSchedule = new OozieSchedule(LocalDateTime.of(2077,7,4,8,15),
                LocalDateTime.of(2077,7,6,8,15), ZoneId.of("UTC"));
        OozieFrequency frequency = new OozieFrequency();
        frequency.setAmount(10L);
        frequency.setUnit("minutes");

        oozieSchedule.setFrequency(frequency);
        sqoopScheduledImportJob.setSchedule(oozieSchedule);

        SqoopImport sqoopImport = new SqoopImport();
        sqoopImport.setJdbcUri("FAKE_JDBC_URI");
        sqoopImport.setTable("table_in_database");
        sqoopImport.setImportMode("incremental");
        sqoopImport.setCheckColumn("id");
        sqoopScheduledImportJob.setSqoopImport(sqoopImport);
        UUID orgId = UUID.fromString("1981838e-bcc9-4402-95eb-60c7f3ca6fbc");

        oozieService.sqoopScheduledImportJob(sqoopScheduledImportJob, orgId);

        assertTrue(sqoopImport.getAppend());
        assertTrue(sqoopImport.getIncremental());
        assertFalse(sqoopImport.getOverwrite());
    }

    @Test
    public void createValidXMLConfigWithSchema() throws IOException {
        SqoopScheduledImportJob sqoopScheduledImportJob = new SqoopScheduledImportJob();
        sqoopScheduledImportJob.setName("test");
        OozieSchedule oozieSchedule = new OozieSchedule(LocalDateTime.of(2077,7,4,8,15),
                LocalDateTime.of(2077,7,6,8,15), ZoneId.of("UTC"));
        OozieFrequency frequency = new OozieFrequency();
        frequency.setAmount(10L);
        frequency.setUnit("minutes");

        oozieSchedule.setFrequency(frequency);
        sqoopScheduledImportJob.setSchedule(oozieSchedule);

        SqoopImport sqoopImport = new SqoopImport();
        sqoopImport.setJdbcUri("FAKE_JDBC_URI");
        sqoopImport.setTable("table_in_database");
        sqoopImport.setImportMode("append");
        sqoopImport.setSchema("my_schema");
        sqoopImport.setUsername("john");
        sqoopImport.setPassword("doe");
        sqoopScheduledImportJob.setSqoopImport(sqoopImport);
        UUID orgId = UUID.fromString("1981838e-bcc9-4402-95eb-60c7f3ca6fbc");

        oozieService.sqoopScheduledImportJob(sqoopScheduledImportJob, orgId);

        String generatedWorkflow = InMemoryOrgSpecificSpace.getWorkflowXml().replaceAll("[ \t\r]","").trim();
        String generatedCoordinator = InMemoryOrgSpecificSpace.getCoordinatorXml().replaceAll("[ \t\r]", "").trim();

        String validWorkflow = FileLoader.readFileResourceNormalized("/workflow_with_schema.xml");
        String validCoordinator = FileLoader.readFileResourceNormalized("/coordinator.xml");

        String workflowDiff = StringUtils.difference(generatedWorkflow.trim(), validWorkflow.trim());
        String coordinatorDiff = StringUtils.difference(generatedCoordinator.trim(),validCoordinator.trim());

        System.out.println("Workflow difference: " + workflowDiff);
        System.out.println("Coordinator difference: " + coordinatorDiff);

        assertTrue(workflowDiff.length() == 0);
        assertTrue(coordinatorDiff.length() == 0);
    }

    @Test
    public void createValidXMLConfigPostgresDriver() throws IOException {
        SqoopScheduledImportJob sqoopScheduledImportJob = new SqoopScheduledImportJob();
        sqoopScheduledImportJob.setName("test");
        OozieSchedule oozieSchedule = new OozieSchedule(LocalDateTime.of(2077,7,4,8,15),
                LocalDateTime.of(2077,7,6,8,15), ZoneId.of("UTC"));
        OozieFrequency frequency = new OozieFrequency();
        frequency.setAmount(10L);
        frequency.setUnit("minutes");

        oozieSchedule.setFrequency(frequency);

        sqoopScheduledImportJob.setSchedule(oozieSchedule);

        SqoopImport sqoopImport = new SqoopImport();
        sqoopImport.setJdbcUri("jdbc:postgresql");
        sqoopImport.setTable("table_in_database");
        sqoopImport.setImportMode("append");
        sqoopImport.setUsername("john");
        sqoopImport.setPassword("doe");
        sqoopScheduledImportJob.setSqoopImport(sqoopImport);
        UUID orgId = UUID.fromString("1981838e-bcc9-4402-95eb-60c7f3ca6fbc");

        oozieService.sqoopScheduledImportJob(sqoopScheduledImportJob, orgId);

        String generatedWorkflow = InMemoryOrgSpecificSpace.getWorkflowXml().replaceAll("[ \t\r]","").trim();
        String generatedCoordinator = InMemoryOrgSpecificSpace.getCoordinatorXml().replaceAll("[ \t\r]", "").trim();

        String validWorkflow = FileLoader.readFileResourceNormalized("/workflow_postgresql.xml");
        String validCoordinator = FileLoader.readFileResourceNormalized("/coordinator.xml");

        String workflowDiff = StringUtils.difference(generatedWorkflow.trim(), validWorkflow.trim());
        String coordinatorDiff = StringUtils.difference(generatedCoordinator.trim(),validCoordinator.trim());

        System.out.println("Workflow difference: " + workflowDiff);
        System.out.println("Coordinator difference: " + coordinatorDiff);

        assertTrue(workflowDiff.length() == 0);
        assertTrue(coordinatorDiff.length() == 0);
    }
}