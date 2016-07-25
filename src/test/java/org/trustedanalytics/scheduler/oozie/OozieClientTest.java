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
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.Environment;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.trustedanalytics.scheduler.client.OozieClient;
import org.trustedanalytics.scheduler.utils.FileLoader;
import org.trustedanalytics.scheduler.utils.MockRestTemplate;

import static org.junit.Assert.assertTrue;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes=TestConfiguration.class)
public class OozieClientTest  {

    @Autowired
    OozieClient oozieClient;



    @Test
    public void nop() {
        assertTrue(true);
    }

   // @Test
    public void submitCoordinatedJob() {

        oozieClient.submitCoordinatedJob("jobDefinitionDirectory");
        System.out.println("Get body" + MockRestTemplate.getRequestBody().toString());

        String generatedProperties = MockRestTemplate.getRequestBody().toString().replaceAll("[ \t\r]","").trim();

        String validProperties = FileLoader.readFileResourceNormalized("/properties.xml");
        String propertiesDiff = StringUtils.difference(generatedProperties.trim(), validProperties.trim());

        System.out.println("Properties difference: " + propertiesDiff);
        assertTrue(propertiesDiff.length() == 0);
    }

}
