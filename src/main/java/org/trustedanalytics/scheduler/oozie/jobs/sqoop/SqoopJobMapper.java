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
package org.trustedanalytics.scheduler.oozie.jobs.sqoop;

import org.apache.commons.lang.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.trustedanalytics.scheduler.DatabaseProvider;
import org.trustedanalytics.scheduler.config.Database;
import rx.Observable;

@Component
public class SqoopJobMapper {

    private final Observable<Database> databases;

    @Autowired
    public SqoopJobMapper(DatabaseProvider databaseProvider) {
        this.databases = databaseProvider.getEnabledEngines();
    }

    public void adjust(SqoopScheduledImportJob job) {

        job.setName(job.getName().replace(" ", "_"));

        if (job.getSchedule() != null
                && job.getSchedule().getFrequency() != null
                && job.getSchedule().getFrequency().getUnit() != null) {
            job.getSchedule().getFrequency().setUnit(job.getSchedule().getFrequency().getUnit().toLowerCase());
        }

        if ("overwrite".equalsIgnoreCase(job.getSqoopImport().getImportMode())) {
            job.getSqoopImport().setIncremental(false);
            job.getSqoopImport().setOverwrite(true);
        }

        if ("incremental".equalsIgnoreCase(job.getSqoopImport().getImportMode())) {
            job.getSqoopImport().setIncremental(true);
            job.getSqoopImport().setOverwrite(false);
        }

        Observable.just(job.getSqoopImport()).map(sqoopImport -> {
            if (OracleJobMapper.isOracle(sqoopImport)) {
                OracleJobMapper.transform(sqoopImport);
            } else {
                adjustJdbcStringForAnyDatabaseExceptOracle(sqoopImport);
            }
            return sqoopImport;
        }).subscribe();

        setDriverClassNameIfEmpty(job.getSqoopImport());
    }


    private void adjustJdbcStringForAnyDatabaseExceptOracle(SqoopImport importJob) {
        importJob.setJdbcUri(importJob.getJdbcUri().replace(":@", "://"));
    }

    private void setDriverClassNameIfEmpty(SqoopImport sqoopImport) {
        if (databases != null && StringUtils.isEmpty(sqoopImport.getDriver())) {
            databases
                    .flatMapIterable(db -> db.getDrivers())
                    .filter(driver -> sqoopImport.getJdbcUri().contains(driver.getName()))
                    .limit(1)
                    .subscribe(driver -> sqoopImport.setDriver(driver.getClassName()));
        }
    }

    private Observable.Transformer<SqoopImport, SqoopImport> transform() {
        return sqoopImport -> sqoopImport;
    }
}
