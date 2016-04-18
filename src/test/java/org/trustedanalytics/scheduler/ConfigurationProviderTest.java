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
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.trustedanalytics.scheduler.config.Database;
import org.trustedanalytics.scheduler.oozie.TestConfiguration;
import org.trustedanalytics.scheduler.util.MockTokenProvider;
import rx.Observable;

import java.util.UUID;

import static org.junit.Assert.assertTrue;

/**
 * Created by GER\kbalka on 4/26/16.
 */

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes=TestConfiguration.class)
public class ConfigurationProviderTest {


    WorkflowSchedulerConfigurationProvider provider;

    @Before
    public void prepare() {
        provider = new WorkflowSchedulerConfigurationProvider(Observable.<Database>empty(), new MockTokenProvider());
    }

    @Test
    public void testTimezoneFiltering() {
        WorkflowSchedulerConfigurationEntity conf = provider.getConfiguration(UUID.randomUUID());
        assertTrue(conf.getTimezones().contains("Etc/GMT+5"));
        assertTrue(conf.getTimezones().contains("Europe/Warsaw"));
        assertTrue(conf.getTimezones().contains("GMT"));
    }
}
