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

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;
import org.trustedanalytics.scheduler.oozie.jobs.sqoop.SqoopScheduledImportJob;

@Component
public class OozieJobValidator {

    private final OozieJobTimeValidator timeValidator;

    @Autowired
    public OozieJobValidator(OozieJobTimeValidator timeValidator) {
        this.timeValidator = timeValidator;
    }

    public void validate(SqoopScheduledImportJob job) {
        timeValidator.validate(job.getSchedule());
        if ( StringUtils.isEmpty(job.getSqoopImport().getImportMode()) ){
            throw new IllegalArgumentException("Import mode must not be null");
        }

        if ("incremental".equalsIgnoreCase(job.getSqoopImport().getImportMode()) && StringUtils.isEmpty(job.getSqoopImport().getCheckColumn())) {
                throw new IllegalArgumentException("CheckColumn must be set when using incremental mode");
        }
    }
}
