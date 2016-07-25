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
package org.trustedanalytics.scheduler;

import static org.springframework.http.MediaType.APPLICATION_JSON_VALUE;
import static org.springframework.web.bind.annotation.RequestMethod.GET;

import org.springframework.web.bind.WebDataBinder;
import org.springframework.web.bind.annotation.*;
import org.trustedanalytics.scheduler.client.OozieClient;
import org.trustedanalytics.scheduler.client.OozieCoordinatedJobInformation;
import org.trustedanalytics.scheduler.client.OozieJobId;
import org.trustedanalytics.scheduler.client.OozieJobLogs;
import org.trustedanalytics.scheduler.client.OozieWorkflowJobInformationExtended;
import org.trustedanalytics.scheduler.filtering.OozieJobFilter;
import org.trustedanalytics.scheduler.oozie.jobs.OozieJobValidator;
import org.trustedanalytics.scheduler.persistence.domain.OozieJobEntity;
import org.trustedanalytics.scheduler.persistence.repository.OozieJobRepository;
import org.trustedanalytics.scheduler.oozie.OozieService;
import org.trustedanalytics.scheduler.oozie.jobs.sqoop.SqoopScheduledImportJob;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import io.swagger.annotations.ApiOperation;

import javax.validation.Valid;

@RestController
public class WorkflowSchedulerController {

    private final OozieClient oozieClient;
    private final OozieService oozieService;
    private final OozieJobFilter oozieJobFilter;
    private final OozieJobRepository oozieJobRepository;
    private final WorkflowSchedulerConfigurationProvider configurationProvider;
    private final OozieJobValidator validator;

    @InitBinder
    protected void initBinder(WebDataBinder binder) {
        binder.setValidator(validator);
    }



    @Autowired
    public WorkflowSchedulerController(OozieClient oozieClient,
                                       OozieService oozieService,
                                       OozieJobRepository oozieJobRepository,
                                       OozieJobFilter oozieJobFilter,
                                       WorkflowSchedulerConfigurationProvider configurationProvider,
                                       OozieJobValidator validator) {
        this.oozieClient = oozieClient;
        this.oozieService = oozieService;
        this.oozieJobRepository = oozieJobRepository;
        this.oozieJobFilter = oozieJobFilter;
        this.configurationProvider = configurationProvider;
        this.validator = validator;
    }

    @ApiOperation(
            value = "Schedule coordinated job",
            notes = "Privilege level: Consumer of this endpoint must be a member of specified organization"
    )
    @RequestMapping(value = "/rest/v1/oozie/schedule_job/coordinated", method = RequestMethod.POST)
    public OozieJobId scheduleOozieCoordinatedJob(
            @RequestParam(value="org") Optional<UUID> org,@Valid
            @RequestBody SqoopScheduledImportJob sqoopScheduledImportJob) throws IOException {
        OozieJobId jobId = oozieService.sqoopScheduledImportJob(sqoopScheduledImportJob, org.get());
        oozieJobRepository.save(new OozieJobEntity(jobId.getId(), org.get().toString()));
        return jobId;
    }

    @ApiOperation(
            value = "Get list of coordinated jobs",
            notes = "Privilege level: Consumer of this endpoint must be a member of specified organization"
    )
    @RequestMapping(value = "/rest/v1/oozie/jobs/coordinated", method = RequestMethod.GET)
    public List<OozieCoordinatedJobInformation> getOozieCoordinatedJobsInformation(
            @RequestParam(value = "org") UUID org,
            @RequestParam(value = "unit") Optional<String> unit,
            @RequestParam(value = "amount") Optional<Integer> amount) {
        final String timeUnit = unit.orElse("days");
        final int timeAmount = amount.orElse(1);

        return oozieJobFilter.filterCoordinatorByOrg(oozieClient.getCoordinatedJobs(timeUnit, timeAmount), org);
    }

    @ApiOperation(
            value = "Get list of workflow jobs",
            notes = "Privilege level: Consumer of this endpoint must be a member of specified organization"
    )
    @RequestMapping(value = "/rest/v1/oozie/jobs/workflow", method = RequestMethod.GET)
    public List<OozieWorkflowJobInformationExtended> getOozieWorkflowJobsInformation(
            @RequestParam(value = "org") UUID org,
            @RequestParam(value = "unit") Optional<String> unit,
            @RequestParam(value = "amount") Optional<Integer> amount) {
        final String timeUnit = unit.orElse("days");
        final int timeAmount = amount.orElse(1);

        return oozieJobFilter.filterWorkflowByOrg(oozieClient.getWorkflowJobs(timeUnit, timeAmount), org);
    }

    @ApiOperation(
            value = "Get logs of specified job",
            notes = "Privilege level: Consumer of this endpoint must be a member of specified organization"
    )
    @RequestMapping(value = "/rest/v1/oozie/jobs/{jobId}/logs", method = RequestMethod.GET)
    public OozieJobLogs getJobLog(@PathVariable("jobId") String jobId) {
        return oozieClient.getJobLogs(jobId);
    }

    @ApiOperation(
            value = "Get details of specified workflow job",
            notes = "Privilege level: Consumer of this endpoint must be a member of specified organization"
    )
    @RequestMapping(value = "/rest/v1/oozie/jobs/workflow/{jobId}", method = RequestMethod.GET)
    public OozieWorkflowJobInformationExtended getWorkflowJobDetails(@PathVariable("jobId") String jobId) {
        return oozieClient.getWorkflowJobDetails(jobId);
    }

    @ApiOperation(
            value = "Get details of specified coordinator job",
            notes = "Privilege level: Consumer of this endpoint must be a member of specified organization"
    )
    @RequestMapping(value = "/rest/v1/oozie/jobs/coordinated/{jobId}", method = RequestMethod.GET)
    public OozieCoordinatedJobInformation getCoordinatedJobDetails(@PathVariable("jobId") String jobId) {
        return oozieClient.getCoordinatedJobDetails(jobId);
    }

    @ApiOperation(
            value = "Get graph of specified job",
            notes = "Privilege level: Consumer of this endpoint must be a member of specified organization"
    )
    @RequestMapping(value = "/rest/v1/oozie/jobs/{jobId}/graph", method = RequestMethod.GET)
    public ResponseEntity<byte[]> getJobGraph(@PathVariable("jobId") String jobId) {
        return oozieClient.getJobGraph(jobId);
    }

    @ApiOperation(
            value  = "Get workflow jobs submitted by specified coordinator job",
            notes = "Privilege level: Consumer of this endpoint must be a member of specified organization"
    )
    @RequestMapping(value = "/rest/v1/oozie/jobs/coordinated/{jobId}/submitted", method = RequestMethod.GET)
    public List<OozieWorkflowJobInformationExtended> getWorkflowJobOfCoorinator(
            @PathVariable("jobId") String jobId,
            @RequestParam(value = "page") Optional<Integer> page,
            @RequestParam(value = "len") Optional<Integer> len) {
        final int offset = page.orElse(1);
        final int totalResults = len.orElse(50);

        return oozieClient.getWorkflowJobOfCoordinator(offset, totalResults, jobId);
    }

    @ApiOperation(
            value = "Submit action for specified job",
            notes = "Privilege level: Consumer of this endpoint must be a member of specified organization"
    )
    @RequestMapping(value = "/rest/v1/oozie/jobs/{jobId}/manage", method = RequestMethod.PUT)
    public void manageJob(
            @PathVariable("jobId") String jobId,
            @RequestParam(value = "action", required = true) String action) {
        oozieClient.manageJob(jobId, action);
    }

    @ApiOperation(
            value = "Get workflow scheduler configuration",
            notes = "Privilege level: Consumer of this endpoint must be a member of specified organization"
    )
    @RequestMapping(value = "/rest/v1/oozie/configuration", method = GET, produces = APPLICATION_JSON_VALUE)
    public WorkflowSchedulerConfigurationEntity getConfiguration(
        @RequestParam(value = "org") UUID org) {
        return configurationProvider.getConfiguration(org);
    }
}
