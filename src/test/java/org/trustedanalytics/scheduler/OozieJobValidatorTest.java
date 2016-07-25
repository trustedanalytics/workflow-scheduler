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
import org.trustedanalytics.scheduler.oozie.jobs.OozieJobValidator;
import org.trustedanalytics.scheduler.oozie.serialization.OozieJobScheduleValidator;
import org.trustedanalytics.scheduler.oozie.OozieFrequency;
import org.trustedanalytics.scheduler.oozie.OozieSchedule;
import org.trustedanalytics.scheduler.oozie.jobs.sqoop.SqoopImport;
import org.trustedanalytics.scheduler.oozie.jobs.sqoop.SqoopScheduledImportJob;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.ZoneId;

@RunWith(MockitoJUnitRunner.class)
public class OozieJobValidatorTest {

    OozieJobValidator validator;

    @Before
    public void setUp() {
         validator = new OozieJobValidator(new OozieJobScheduleValidator());
    }

    @Test(expected = IllegalArgumentException.class)
    public void importModeIncrementalMissingCheckColumn_throws_exception() throws IOException {
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
        sqoopScheduledImportJob.setSqoopImport(sqoopImport);

        validator.validate(sqoopScheduledImportJob);

    }

}
