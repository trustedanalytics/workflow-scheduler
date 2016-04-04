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
package org.trustedanalytics.scheduler.filtering;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;
import org.trustedanalytics.scheduler.client.OozieCoordinatedJobInformation;
import org.trustedanalytics.scheduler.client.OozieWorkflowJobInformationExtended;
import org.trustedanalytics.scheduler.persistence.repository.OozieJobRepository;

import java.util.List;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;

@Component
public class OozieJobFilter {

    private final OozieJobRepository oozieJobRepository;

    @Autowired
    public OozieJobFilter(OozieJobRepository oozieJobRepository) {
        this.oozieJobRepository = oozieJobRepository;
    }

    public List<OozieCoordinatedJobInformation> filterCoordinatorByOrg(List<OozieCoordinatedJobInformation> coordinatedJobs, UUID org) {
        return byOrg(coordinatedJobs, org, job -> job.getCoordJobId());
    }

    public List<OozieWorkflowJobInformationExtended> filterWorkflowByOrg(List<OozieWorkflowJobInformationExtended> workflowJobs, UUID org) {
        return byOrg(workflowJobs, org, job -> StringUtils.isEmpty(job.getCoordinatorId()) ? job.getId() : job.getCoordinatorId());
    }

    private <T> List<T> byOrg(List<T> jobs, UUID org, Function<T, String> func) {
        return jobs.stream()
            .filter(job -> oozieJobRepository
                .findByOrgId(org.toString())
                .stream()
                .map(entity -> entity.getJobId())
                .collect(Collectors.toSet())
                .contains(func.apply(job)))
            .collect(Collectors.toList());
    }
}
