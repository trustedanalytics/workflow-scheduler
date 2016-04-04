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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.trustedanalytics.scheduler.client.OozieClient;
import org.trustedanalytics.scheduler.config.Database;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;

import org.simpleframework.xml.Serializer;
import org.simpleframework.xml.core.Persister;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;

import java.io.IOException;
import java.io.InputStream;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Collection;

import org.trustedanalytics.scheduler.filesystem.HdfsConfigProvider;
import org.trustedanalytics.scheduler.rest.RestOperationsFactory;
import org.trustedanalytics.scheduler.security.TokenProvider;
import rx.Observable;

@Configuration
public class WorkflowSchedulerConfiguration {

    private static final Logger LOGGER = LoggerFactory.getLogger(WorkflowSchedulerConfiguration.class);

    @Autowired
    private RestOperationsFactory restOperationsFactory;

    @Autowired
    private HdfsConfigProvider hdfsConfigProvider;

    @Autowired
    private TokenProvider oauthTokenProvider;

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


    @Bean
    public OozieClient oozieClient() throws IOException {
        return new OozieClient(restOperationsFactory, hdfsConfigProvider, oauthTokenProvider);
    }

    @Bean
    public Observable<Database> databaseEngines() {
        try (final InputStream input = new ClassPathResource("databases.json").getInputStream()) {
            TypeReference<Collection<Database>> type = new TypeReference<Collection<Database>>() {};
            Collection<Database> databases = objectMapper().readValue(input, type);
            return Observable.from(databases);
        } catch (IOException e) {
            LOGGER.error("Exception while reading databases: ", e);
            throw new IllegalStateException("Unable to load supported databases");
        }
    }

    @Bean
    public Serializer serializer() {
        return new Persister();
    }
}
