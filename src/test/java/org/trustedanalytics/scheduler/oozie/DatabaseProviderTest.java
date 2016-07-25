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

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.mock.env.MockEnvironment;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.trustedanalytics.scheduler.DatabaseProvider;
import org.trustedanalytics.scheduler.config.Database;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes=TestConfiguration.class)
public class DatabaseProviderTest {

    @Test
    public void filterOnlyOracleConfiguration() {
        MockEnvironment mockEnv = new MockEnvironment();
        mockEnv.setProperty("sqoop.database.oracle", "true");
        DatabaseProvider engines = new DatabaseProvider(mockEnv, new ObjectMapper());
        List<Database> dbs = engines.getEnabledEngines().toList().toBlocking().single();
        assertEquals(dbs.size(), 1);
        assertTrue(dbs.get(0).getName().toLowerCase().equals("oracle"));
    }

}
