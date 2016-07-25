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
package org.trustedanalytics.scheduler.client;

import com.google.common.base.Strings;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.HttpMethod;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Component;
import org.trustedanalytics.scheduler.filesystem.HdfsConfigProvider;
import org.trustedanalytics.scheduler.oozie.serialization.JobContext;
import org.trustedanalytics.scheduler.rest.RestOperationsFactory;
import org.trustedanalytics.scheduler.security.TokenProvider;
import org.trustedanalytics.scheduler.utils.OozieNameResolver;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

@Component
public class OozieClient {

    private static final Logger LOGGER = LoggerFactory.getLogger(OozieClient.class);
    private static final String JOBS_URL = "/oozie/v1/jobs";
    private static final String SINGLE_JOB_URL = "/oozie/v1/job/";


    private RestOperationsFactory restTemplateFactory;
    private TokenProvider tokenProvider;

    private JobContext jobContext;

    @Autowired
    public OozieClient(RestOperationsFactory restOperationsFactory, TokenProvider tokenProvider, JobContext jobContext) {
        this.restTemplateFactory = restOperationsFactory;
        this.tokenProvider = tokenProvider;
        this.jobContext = jobContext;
    }

    public List<OozieWorkflowJobInformationExtended> getWorkflowJobs(String unit, int amount) {

        int offset = 0;
        int len = 50;
        LocalDateTime searchedTime = getSearchedDate(unit, amount);
        List<OozieWorkflowJobInformationExtended> oozieWorkflowJobsInformationExtended = new ArrayList<>();

        do {
            oozieWorkflowJobsInformationExtended.addAll(
                getJobs(offset, len, "wf", new ParameterizedTypeReference<Page<OozieWorkflowJobInformation>>() {})
                    .stream()
                    .filter(job -> getDate(job.getCreatedTime()).isAfter(searchedTime))
                    .map(job -> new OozieWorkflowJobInformationExtended(job, jobContext.getNameNode()))
                    .collect(Collectors.toList()));
            offset += len;
        } while(oozieWorkflowJobsInformationExtended.size() == offset);
        return oozieWorkflowJobsInformationExtended;
    }

    public List<OozieCoordinatedJobInformation> getCoordinatedJobs(String unit, int amount) {

        int offset = 0;
        int len = 2000;
        LocalDateTime searchedTime = getSearchedDate(unit, amount);
        List<OozieCoordinatedJobInformation> oozieCoordinatedJobInformations = new ArrayList<>();

        do {
            oozieCoordinatedJobInformations.addAll(
                getJobs(offset, len, "coord", new ParameterizedTypeReference<Page<OozieCoordinatedJobInformation>>() {})
                    .stream()
                    .filter(job -> Strings.isNullOrEmpty(job.getLastAction()) || getDate(job.getLastAction()).isAfter(searchedTime))
                    .collect(Collectors.toList()));
            offset += len;
        } while(oozieCoordinatedJobInformations.size() == offset);
        return oozieCoordinatedJobInformations;
    }

    private static LocalDateTime getDate(String inputDate) {

        SimpleDateFormat formatter = new SimpleDateFormat("EE, dd MMM yyyy HH:mm:ss");
        Date date = null;
        try {
            date = formatter.parse(inputDate.substring(0, inputDate.lastIndexOf(' ')));
        } catch (ParseException e) {
            LOGGER.error(e.toString());
            throw new IllegalArgumentException("Could not parse date: " + inputDate);
        }
        String timezone = inputDate.substring(inputDate.lastIndexOf(' ') + 1);

        return LocalDateTime.ofInstant(date.toInstant(), ZoneId.of(timezone));
    }

    private LocalDateTime getSearchedDate(String unit, int amount) {
        LocalDateTime currentTime = LocalDateTime.ofInstant(new Date().toInstant(), ZoneId.of("GMT"));

        if("days".equals(unit)) {
            return currentTime.minusDays(amount);
        }
        if("weeks".equals(unit)) {
            return currentTime.minusWeeks(amount);
        }
        if("hours".equals(unit)) {
            return currentTime.minusHours(amount);
        }
        return currentTime.minusDays(1);
    }

    private <T> List<T> getJobs(int offset, int len, String jobType, ParameterizedTypeReference<Page<T>> responseType) {
        return getForEntity(jobContext.getOozieApiUrl() + JOBS_URL + "?jobtype=" + jobType + "&len=" + len + "&offset=" + offset, responseType)
            .getEntries();
    }

    private <T> List<T> getJobs(String appName, String jobType, ParameterizedTypeReference<Page<T>> responseType) {
        final String name = OozieNameResolver.resolveWorkflowAppName(appName);
        return getForEntity(jobContext.getOozieApiUrl() + JOBS_URL + "?filter=name=" + name + "&len=20000&jobtype=" + jobType, responseType)
            .getEntries();
    }

    private <T> Page<T> getForEntity(String url, ParameterizedTypeReference<Page<T>> parameterizedTypeReference) {
        return restTemplateFactory.getRestTemplate().exchange(url, HttpMethod.GET, null, parameterizedTypeReference).getBody();
    }

    public OozieJobId submitCoordinatedJob(String jobDefinitionDirectory) {
        return submitJob(getRequestBody(tokenProvider.getUserId(), jobDefinitionDirectory, "oozie.coord.application.path"));
    }

    public OozieJobId submitWorkflowJob(String jobDefinitionDirectory) {
        return submitJob(getRequestBody(tokenProvider.getUserId(), jobDefinitionDirectory, "oozie.wf.application.path"));
    }

    public OozieJobLogs getJobLogs(String jobId) {
        return new OozieJobLogs(restTemplateFactory.getRestTemplate().getForEntity(jobContext.getOozieApiUrl() + SINGLE_JOB_URL + jobId + "?show=log", String.class).getBody());
    }

    private <T> T getJobDetails(String jobId, ParameterizedTypeReference<T> parameterizedTypeReference) {
        return restTemplateFactory.getRestTemplate().exchange(jobContext.getOozieApiUrl() + SINGLE_JOB_URL + jobId, HttpMethod.GET, null, parameterizedTypeReference).getBody();
    }

    public OozieWorkflowJobInformationExtended getWorkflowJobDetails(String jobId) {
        return new OozieWorkflowJobInformationExtended(getJobDetails(jobId, new ParameterizedTypeReference<OozieWorkflowJobInformation>() {}), jobContext.getNameNode());
    }

    public OozieCoordinatedJobInformation getCoordinatedJobDetails(String jobId) {
        return getJobDetails(jobId, new ParameterizedTypeReference<OozieCoordinatedJobInformation>() {});
    }

    public void manageJob(String jobId, String action) {
        restTemplateFactory.getRestTemplate().put(jobContext.getOozieApiUrl() + SINGLE_JOB_URL + jobId + "?action=" + action, null, String.class);
    }

    public List<OozieWorkflowJobInformationExtended> getWorkflowJobOfCoordinator(int offset, int len, String jobId) {
        return getJobs(getCoordinatedJobDetails(jobId).getCoordJobName(), "wf", new ParameterizedTypeReference<Page<OozieWorkflowJobInformation>>() {})
            .stream()
            .filter(job -> Objects.nonNull(job.getParentId()) && job.getParentId().contains(jobId))
            .map(job -> new OozieWorkflowJobInformationExtended(job, jobContext.getNameNode()))
            .skip((offset - 1)*len)
            .limit(len)
            .collect(Collectors.toList());
    }

    public ResponseEntity<byte[]> getJobGraph(String jobId) {

        ResponseEntity<byte[]> response = restTemplateFactory.getRestTemplate().getForEntity(jobContext.getOozieApiUrl() + SINGLE_JOB_URL + jobId + "?show=graph", byte[].class);

        final HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.IMAGE_PNG);

       return new ResponseEntity<>(response.getBody(), headers, HttpStatus.MULTI_STATUS.ACCEPTED);
    }

    private OozieJobId submitJob(String requestBody) {

        Map<String, String> jobProperties = new HashMap<>();
        jobProperties.put("Content-Type", "application/xml;charset=UTF-8");

        // header
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_XML);
        HttpEntity<String> entity = new HttpEntity<>(requestBody, headers);


        return restTemplateFactory.getRestTemplate().postForEntity(jobContext.getOozieApiUrl() + JOBS_URL, entity, OozieJobId.class, jobProperties).getBody();
    }

    private String getRequestBody(String userName, String jobDefinitionDirectory, String jobType) {

        return "<?xml version=\"1.0\" encoding=\"UTF-8\"?> " +
                "<configuration>" +
                property("user.name", userName) +
                property(jobType, jobDefinitionDirectory) +
                property("oozie.use.system.libpath", "true") +
                property("nameNode", jobContext.getNameNode()) +
                property("queueName", "default") +
                property("jobTracker", jobContext.getJobTracker()) +
        "</configuration>";
    }

    private static String property(String name, String value) {
        return "<property><name>" + name + "</name><value>" + value + "</value></property>";
    }
}
