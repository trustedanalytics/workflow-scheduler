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

import org.mockito.Mock;
import org.mockito.Mockito;
import org.simpleframework.xml.Serializer;
import org.simpleframework.xml.core.Persister;
import org.springframework.beans.factory.config.PropertyPlaceholderConfigurer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.trustedanalytics.scheduler.OozieJobMapper;
import org.trustedanalytics.scheduler.OozieJobTimeValidator;
import org.trustedanalytics.scheduler.OozieJobValidator;
import org.trustedanalytics.scheduler.client.OozieClient;
import org.trustedanalytics.scheduler.filesystem.LocalHdfsConfigProvider;
import org.trustedanalytics.scheduler.util.ConstantJobIdSupplier;
import org.trustedanalytics.scheduler.util.InMemoryOrgSpecificSpaceFactory;
import org.trustedanalytics.scheduler.util.MockRestOperationsFactory;
import org.trustedanalytics.scheduler.util.MockTokenProvider;

import java.io.IOException;
import java.util.Properties;

@Configuration
public class TestConfiguration {

    /*
     *  create sqoop.metastore property for unit test
     *
     */

    @Bean
    public static PropertyPlaceholderConfigurer propertyPlaceholderConfigurer() {
        PropertyPlaceholderConfigurer ppc = new PropertyPlaceholderConfigurer();
        ppc.setIgnoreResourceNotFound(true);
        final Properties properties = new Properties();
        properties.setProperty("sqoop.metastore", "test_metastore_url:32158");
        properties.setProperty("hadoop.home", "test_hadoop_home");
        properties.setProperty("job.tracker", "test_job_tracker");
        properties.setProperty("oozie.api.url", "oozie_api_url");
        ppc.setProperties(properties);
        return ppc;
    }

    @Mock
    private OozieClient oozieClient;

    private Serializer serializer = new Persister();

    @Bean
    public OozieService getOozieService() {

        oozieClient = Mockito.mock(OozieClient.class);
        return new OozieService(new InMemoryOrgSpecificSpaceFactory(),
                oozieClient,
                serializer,
                new ConstantJobIdSupplier(),
                new OozieJobValidator(new OozieJobTimeValidator()),
                new OozieJobMapper()
                );
    }

    @Bean
    public OozieClient getOozieClient() throws IOException {
        return new OozieClient(new MockRestOperationsFactory(), new LocalHdfsConfigProvider(), new MockTokenProvider());
    }


}

