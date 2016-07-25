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
import org.trustedanalytics.scheduler.oozie.serialization.WorkflowInstance.WorkflowInstanceBuilder;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class SqoopNode implements XmlNode {

    private final String name;
    private final String then;
    private final String onError;
    private final Map<String, String> properties;
    private final String command;
    private final JobContext jobContext;
    private final List<String> files;

    public SqoopNode(WorkflowActionNodeBuilder builder) {
        this.name = Objects.requireNonNull(builder.name, "name");
        this.then = Objects.requireNonNull(builder.then, "then");
        this.onError = Objects.requireNonNull(builder.onError, "onError");
        this.properties = Objects.requireNonNull(builder.properties, "properties");
        this.command = Objects.requireNonNull(builder.command, "command");
        this.jobContext = Objects.requireNonNull(builder.jobContext, "jobContext");
        this.files = Objects.requireNonNull(builder.files, "files");
    }

    public static WorkflowActionNodeBuilder builder(JobContext jobContext) {
        return new WorkflowActionNodeBuilder(jobContext);
    }

    @Override
    public XMLBuilder2 asXmlBuilder() {
            XMLBuilder2 builder =  XMLBuilder2.create("action").a("name", name)
                    .e("sqoop").ns("uri:oozie:sqoop-action:0.4")
                    .e("job-tracker").t(jobContext.getJobTracker()).up()
                    .e("name-node").t(jobContext.getNameNode()).up()
                    .e("prepare").up()
                     .importXMLBuilder(configurationXml())
                    .e("command").t(command).up();
            for (String file : files) {
                builder.e("file").t(file).up();
            }
            builder.up().up().e("ok").a("to",then).up()
                    .e("error").a("to", onError);
        return builder;
    }

    private XMLBuilder2 configurationXml() {
        XMLBuilder2 configBuilder = XMLBuilder2.create("configuration");
        for (Map.Entry<String,String> entry: properties.entrySet()) {
            configBuilder.e("property").e("name").t(entry.getKey()).up()
                                        .e("value").t(entry.getValue()).up()
                        .up()
                    .up();
        }
        return configBuilder;
    }

    public static class WorkflowActionNodeBuilder implements BuilderNode {
        private WorkflowInstanceBuilder parent;
        private String name;
        private String then;
        private String onError;
        private String command;
        private Map<String, String> properties = new HashMap<>();
        private JobContext jobContext;
        public List<String> files = new ArrayList<>();

        public WorkflowActionNodeBuilder(JobContext jobContext) {
            this.properties.put("mapred.job.queue.name", jobContext.getQueueName());
            this.jobContext = jobContext;
        }

        public SqoopNode build() {
            return new SqoopNode(this);
        }

        protected WorkflowActionNodeBuilder setParent(WorkflowInstanceBuilder parent) {
            this.parent = parent;
            return this;
        }

        public WorkflowActionNodeBuilder then(String then) {
            this.then = then;
            return this;
        }

        public WorkflowActionNodeBuilder onError(String onError) {
            this.onError = onError;
            return this;
        }

        public WorkflowActionNodeBuilder setName(String name) {
            this.name = name;
            return this;
        }

        public WorkflowActionNodeBuilder setCommand(String command) {
            this.command = command;
            return this;
        }

        public WorkflowActionNodeBuilder addFile(String filePath) {
            this.files.add(filePath);
            return this;
        }

        public WorkflowInstanceBuilder and() {
            return parent;
        }
    }
}
