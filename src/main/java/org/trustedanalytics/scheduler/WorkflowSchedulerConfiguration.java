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

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.trustedanalytics.scheduler.oozie.serialization.JobContext;

import java.time.LocalDateTime;
import java.time.ZoneId;

@Configuration
public class WorkflowSchedulerConfiguration {

    private static final Logger LOGGER = LoggerFactory.getLogger(WorkflowSchedulerConfiguration.class);

    @Value("${job.tracker}")
    private String jobTracker;

    @Value("${sqoop.metastore}")
    private String sqoopMetastore;

    @Value("${namenode}")
    private String namenode;

    @Value("${oozie.api.url:host}")
    private String oozieApiUrl;


    @Bean
    public JobContext jobContext() {
        LOGGER.info("Creating job context from env");
        return JobContext.builder()
            .jobTracker(jobTracker)
            .sqoopMetastore(sqoopMetastore)
            .nameNode(namenode)
            .oozieApiUrl(oozieApiUrl)
            .build();
    }

    @Bean
    public ObjectMapper objectMapper() {
        final ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);

        final SimpleModule simpleModule = new SimpleModule();
        simpleModule.addDeserializer(ZoneId.class, new ZoneIdDeserializer());
        simpleModule.addDeserializer(LocalDateTime.class, new LocalDateTimeDeserializer());
        objectMapper.registerModules(new Jdk8Module(), simpleModule);

        return objectMapper;
    }

}
