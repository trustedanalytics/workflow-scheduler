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
package org.trustedanalytics.scheduler.oozie.serialization;

import com.jamesmurty.utils.XMLBuilder2;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.trustedanalytics.scheduler.oozie.OozieSchedule;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.Properties;

public class CoordinatorInstance {

    private static final Logger LOGGER = LoggerFactory.getLogger(CoordinatorInstance.class);

    private final String name;
    private final String targetDir;
    private final String oozieLibpath;
    private final String appPath;
    private final String frequency;
    private final String start;
    private final String end;
    private final String timezone;
    private final Boolean oozieUseSystemLibpath;
    private final JobContext jobContext;

    private CoordinatorInstance(CoordinatorInstanceBuilder builder) {
        this.name = Objects.requireNonNull(builder.name, "name");
        this.targetDir = Objects.requireNonNull(builder.targetDir, "targetDir");
        this.oozieLibpath = Objects.requireNonNull(builder.oozieLibpath, "oozieLibpath");
        this.oozieUseSystemLibpath = Objects.requireNonNull(builder.oozieUseSystemLibpath, "oozieUseSystemLibpath");
        this.appPath = Objects.requireNonNull(builder.appPath, "appPath");
        this.frequency = Objects.requireNonNull(builder.frequency, "frequency");
        this.start = Objects.requireNonNull(builder.start, "start");
        this.end = Objects.requireNonNull(builder.end, "end");
        this.timezone = Objects.requireNonNull(builder.timezone, "timezone");
        this.jobContext = Objects.requireNonNull(builder.jobContext, "jobContext");
    }

    public static CoordinatorInstanceBuilder builder(JobContext jobContext) {
        return new CoordinatorInstanceBuilder(jobContext);
    }

    public static class CoordinatorInstanceBuilder {
        private String name;
        private String targetDir;
        private String oozieLibpath;
        private String appPath;
        private String frequency;
        private String start;
        private String end;
        private String timezone;
        private Boolean oozieUseSystemLibpath;
        private JobContext jobContext;

        public CoordinatorInstanceBuilder (JobContext jobContext) {
            this.jobContext = jobContext;
        }

        public CoordinatorInstanceBuilder setName(String name) {
            this.name = name;
            return this;
        }

        public CoordinatorInstanceBuilder setTargetDir(String targetDir) {
            this.targetDir = targetDir;
            return this;
        }

        public CoordinatorInstanceBuilder setOozieLibpath(String oozieLibpath) {
            this.oozieLibpath = oozieLibpath;
            return this;
        }

        public CoordinatorInstanceBuilder setOozieUseSystemLibpath(boolean oozieUseSystemLibpath) {
            this.oozieUseSystemLibpath = oozieUseSystemLibpath;
            return this;
        }

        public CoordinatorInstanceBuilder setAppPath(String appPath) {
            this.appPath = appPath;
            return this;
        }

        public CoordinatorInstanceBuilder setOozieSchedule(OozieSchedule oozieSchedule) {
            this.start = oozieSchedule.coordinatorStart();
            this.end = oozieSchedule.coordinatorEnd();
            this.timezone = oozieSchedule.coordinatorZone();

            if((Objects.equals(oozieSchedule.getFrequency().getUnit(), "minutes") && oozieSchedule.getFrequency().getAmount() >= 5)
                || (!Objects.equals(oozieSchedule.getFrequency().getUnit(), "minutes") && oozieSchedule.getFrequency().getAmount() > 0)) {
                // @formatter:off
                this.frequency = String.format("${coord:%s(%s)}", oozieSchedule.getFrequency().getUnit(), oozieSchedule.getFrequency().getAmount());
                // @formatter:on
            } else {
                throw new IllegalArgumentException("Coordinator frequency must be bigger then 5 minutes");
            }
            return this;
        }

        public CoordinatorInstance build() {
            return new CoordinatorInstance(this);
        }
    }

    public InputStream asStream() {
        Properties outputProperties = new Properties();
        outputProperties.put(javax.xml.transform.OutputKeys.OMIT_XML_DECLARATION, "yes");
        outputProperties.put(javax.xml.transform.OutputKeys.METHOD, "xml");
        outputProperties.put(javax.xml.transform.OutputKeys.INDENT, "yes");
        outputProperties.put("{http://xml.apache.org/xslt}indent-amount", "2");

        String xml = XMLBuilder2.create("coordinator-app")
                    .a("name", name)
                    .a("frequency", frequency)
                    .a("start", start)
                    .a("end", end)
                    .a("timezone", timezone)
                .ns("uri:oozie:coordinator:0.4")
                .e("action")
                  .e("workflow")
                    .e("app-path").t(appPath)
                      .up()
                    .e("configuration")
                      .importXMLBuilder(property("jobTracker", jobContext.getJobTracker()))
                      .importXMLBuilder(property("nameNode", jobContext.getNameNode()))
                      .importXMLBuilder(property("queueName", jobContext.getQueueName()))
                      .importXMLBuilder(property("targetDir", targetDir))
                      .importXMLBuilder(property("oozie.libpath", oozieLibpath))
                      .importXMLBuilder(property("oozie.use.system.libpath", oozieUseSystemLibpath.toString()))
                .asString(outputProperties);

        LOGGER.info("Generated coordinator.xml: {}", xml);
        return IOUtils.toInputStream(xml, StandardCharsets.UTF_8);
    }

    private XMLBuilder2 property(String name, String value) {
        return XMLBuilder2.create("property").e("name").t(name).up()
            .e("value").t(value).up()
            .up();
    }
}
