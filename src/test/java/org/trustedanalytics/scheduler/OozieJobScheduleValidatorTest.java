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

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;
import org.trustedanalytics.scheduler.oozie.serialization.OozieJobScheduleValidator;
import org.trustedanalytics.scheduler.oozie.OozieSchedule;
import org.trustedanalytics.scheduler.oozie.jobs.sqoop.SqoopScheduledImportJob;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.ZoneId;

@RunWith(MockitoJUnitRunner.class)
public class OozieJobScheduleValidatorTest {

    OozieJobScheduleValidator oozieJobScheduleValidator;

    @Before
    public void setUp() {
         oozieJobScheduleValidator = new OozieJobScheduleValidator();
    }

    @Test(expected = IllegalArgumentException.class)
    public void should_throwIllegalArgumentException_when_startTimeIsLaterThenEndTime() throws IOException {
        SqoopScheduledImportJob sqoopScheduledImportJob = new SqoopScheduledImportJob();
        sqoopScheduledImportJob.setSchedule(new OozieSchedule(LocalDateTime.of(2016, 2, 25, 15, 55), LocalDateTime.of(2016, 2, 25, 15, 54), ZoneId.of("UTC")));
        oozieJobScheduleValidator.validate(sqoopScheduledImportJob.getSchedule());
    }

}
