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


import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.collect.ImmutableList;
import org.simpleframework.xml.Serializer;
import org.simpleframework.xml.core.Persister;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.core.env.Environment;
import org.springframework.core.io.ClassPathResource;
import org.springframework.stereotype.Component;
import org.trustedanalytics.scheduler.config.Database;
import rx.Observable;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import com.fasterxml.jackson.databind.ObjectMapper;

@Component
public class DatabaseProvider {

    private static final Logger LOGGER = LoggerFactory.getLogger(DatabaseProvider.class);
    private final Environment env;
    private final ObjectMapper objectMapper;
    private static final List<String> ALL_DATABASES = ImmutableList.of("postgresql", "oracle", "mysql", "teradata");

    private final static String SQOOP_DB_PROPERTY_PREFIX = "sqoop.database";

    @Autowired
    public DatabaseProvider(Environment env, ObjectMapper objectMapper) {
        this.env = Objects.requireNonNull(env,"env");
        this.objectMapper = objectMapper;
    }

    public Observable<Database> getEnabledEngines() {
        try (final InputStream input = new ClassPathResource("databases.json").getInputStream()) {
            TypeReference<Collection<Database>> type = new TypeReference<Collection<Database>>() {};
            Collection<Database> databases = objectMapper.readValue(input, type);
            return Observable.from(databases).filter(db -> enabledDatabasesFromEnv().contains(db.getName().toLowerCase()));
        } catch (IOException e) {
            LOGGER.error("Exception while reading databases: ", e);
            throw new IllegalStateException("Unable to load supported databases");
        }
    }

    @Bean
    public Serializer serializer() {
        return new Persister();
    }

    private Collection<String> enabledDatabasesFromEnv() {
        return ALL_DATABASES.stream().filter(db ->  isEnabledFromEnv(db)).collect(Collectors.toList());
    }

    private Boolean isEnabledFromEnv(String db) {
        return "true".equalsIgnoreCase(env.getProperty(SQOOP_DB_PROPERTY_PREFIX + "." + db));
    }
}
