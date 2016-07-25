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
import org.trustedanalytics.scheduler.oozie.serialization.SqoopNode.WorkflowActionNodeBuilder;
import org.trustedanalytics.scheduler.oozie.serialization.CreateFileNode.WorkflowCreateFileBuilder;
import org.trustedanalytics.scheduler.oozie.serialization.DecisionNode.WorkflowDecisionNodeBuilder;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Properties;

public class WorkflowInstance {

    private final String workflowName;
    private final String errorMsg;
    private final String killNodeName;
    private final String startNodeName;

    private List<BuilderNode> actionNodes = new ArrayList<>();

    public static final Properties xmlProperties = new Properties()  {
        {
            put(javax.xml.transform.OutputKeys.OMIT_XML_DECLARATION, "yes");
            put(javax.xml.transform.OutputKeys.METHOD, "xml");
            put(javax.xml.transform.OutputKeys.INDENT, "yes");
            put("{http://xml.apache.org/xslt}indent-amount", "2");
        }
    };

    private WorkflowInstance(WorkflowInstanceBuilder builder) {
        this.actionNodes = new ArrayList<>(builder.actionNodes);
        this.errorMsg = builder.errorMsg;
        this.killNodeName = Objects.requireNonNull(builder.killNodeName, "killNodeName");
        this.startNodeName = Objects.requireNonNull(builder.startNodeName, "startNodeName");
        this.workflowName = Objects.requireNonNull(builder.workflowName, "workflowName");
    }

    public static WorkflowInstanceBuilder builder(JobContext jobContext) {
        return new WorkflowInstanceBuilder(jobContext);
    }

    public InputStream asStream() {
        XMLBuilder2 builder = XMLBuilder2.create("workflow-app");
        builder.a("name",workflowName);
        builder.ns("uri:oozie:workflow:0.4");
        builder.e("start").a("to",startNodeName).up();
        for (BuilderNode a : actionNodes) {
            builder.importXMLBuilder(a.build().asXmlBuilder());
        }
        builder.e("kill").a("name",killNodeName)
                  .e("message").t(errorMsg).up()
                  .up().e("end").a("name","end");
        String xml =  builder.asString(xmlProperties);
        System.out.println("Xml created : " + xml);
        return IOUtils.toInputStream(xml, StandardCharsets.UTF_8);
    }

    public static class WorkflowInstanceBuilder {
        private String errorMsg;
        private String killNodeName;
        private String startNodeName;
        private List<BuilderNode> actionNodes = new ArrayList<>();
        private JobContext jobContext;
        private String workflowName;

        public WorkflowInstanceBuilder(JobContext jobContext) {
            this.jobContext = jobContext;
        }

        public WorkflowInstance build() {
            return new WorkflowInstance(this);
        }

        public WorkflowActionNodeBuilder sqoopAction() {
            WorkflowActionNodeBuilder childBuilder = SqoopNode.builder(jobContext)
            .setParent(this)
            .onError("fail");
            actionNodes.add(childBuilder);
            return childBuilder;
        }

        public WorkflowDecisionNodeBuilder sqoopFileExistDecision(String flagPath) {
            WorkflowDecisionNodeBuilder childBuilder = DecisionNode.builder()
                .setParent(this)
                .setCondition(String.format("${fs:exists(\"%s\") eq true}", flagPath));
            actionNodes.add(childBuilder);
            return childBuilder;
        }

        public WorkflowInstanceBuilder setStartNode(String startNodeName) {
            this.startNodeName = startNodeName;
            return this;
        }

        public WorkflowInstanceBuilder sqoopKill(String errorMsg) {
            this.errorMsg = errorMsg;
            this.killNodeName = "fail";
            return this;
        }

        public WorkflowCreateFileBuilder createFile() {
            WorkflowCreateFileBuilder createFileBuilder = CreateFileNode.builder().setParent(this);
            actionNodes.add(createFileBuilder);
            return createFileBuilder;
        }

        public WorkflowInstanceBuilder setName(String workflowName) {
            this.workflowName = workflowName;
            return this;
        }
    }
}
