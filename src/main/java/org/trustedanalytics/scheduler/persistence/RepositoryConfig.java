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
package org.trustedanalytics.scheduler.persistence;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.orm.jpa.EntityScan;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.core.env.Environment;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.jdbc.datasource.embedded.EmbeddedDatabaseBuilder;
import org.springframework.jdbc.datasource.embedded.EmbeddedDatabaseType;

import javax.sql.DataSource;

@Configuration
@EnableJpaRepositories(basePackages = "org.trustedanalytics.scheduler.persistence.repository")
@EnableAutoConfiguration
@EntityScan(basePackages = {"org.trustedanalytics.scheduler.persistence.domain"})
public class RepositoryConfig {

    private static final Logger LOGGER = LoggerFactory.getLogger(RepositoryConfig.class);

    @Autowired
    Environment env;

    @Bean
    @Profile("local")
    public DataSource localDataSource() {
        LOGGER.info("Creating H2 data source");
        return new EmbeddedDatabaseBuilder()
                .setName("localdb")
                .setType(EmbeddedDatabaseType.H2)
                .build();
    }

    @Bean
    @Profile("cloud")
    public DataSource cloudDataSource() {

        LOGGER.info("Creating postgresql data source");
        /*
        spring.jpa.database: POSTGRESQL
        spring.datasource: platform=postgres
        spring.jpa.show-sql: true
        spring.jpa.hibernate.ddl-auto: create-drop
        spring.database.driverClassName: org.postgresql.Driver
        spring.datasource.url: jdbc:postgresql://${vcap.services.workflow-scheduler-db.credentials.hostname}:${vcap.services.workflow-scheduler-db.credentials.port}/${vcap.services.workflow-scheduler-db.credentials.dbname}
        spring.datasource.username: ${vcap.services.workflow-scheduler-db.username}
        spring.datasource.password: ${vcap.services.workflow-scheduler-db.credentials.password}
        */
        DriverManagerDataSource dataSource = new DriverManagerDataSource();
        dataSource.setDriverClassName(env.getProperty("spring.database.driverClassName"));
        dataSource.setUrl(env.getProperty("spring.datasource.url"));
        dataSource.setUsername(env.getProperty("spring.datasource.username"));
        dataSource.setPassword(env.getProperty("spring.datasource.password"));
        /*
        Properties connectionProperties = new Properties();
        connectionProperties.put("")
        dataSource.setConnectionProperties(connectionProperties);
        */
        return dataSource;

    }
}

