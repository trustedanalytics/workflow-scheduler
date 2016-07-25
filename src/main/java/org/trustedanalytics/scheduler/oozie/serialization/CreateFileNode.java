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

import java.util.Objects;

public class CreateFileNode implements XmlNode {

    private final String name;
    private final String path;
    private final String then;

    public CreateFileNode(WorkflowCreateFileBuilder builder) {
        this.name = Objects.requireNonNull(builder.name, "name");
        this.path = Objects.requireNonNull(builder.path, "path");
        this.then = Objects.requireNonNull(builder.then, "then");
    }

    public static WorkflowCreateFileBuilder builder() {
        return new WorkflowCreateFileBuilder();
    }

    @Override
    public XMLBuilder2 asXmlBuilder() {
        return XMLBuilder2.create("action").a("name",name).e("fs").e("touchz").a("path",path)
                .up().up().e("ok").a("to", then).up().e("error").a("to","fail");
    }

    public static class WorkflowCreateFileBuilder implements BuilderNode {

        private WorkflowInstanceBuilder parent;
        private String name;
        private String path;
        private String then;

        public CreateFileNode build() {
            return new CreateFileNode(this);
        }

        protected WorkflowCreateFileBuilder setParent(WorkflowInstanceBuilder parent) {
            this.parent = parent;
            return this;
        }

        public WorkflowCreateFileBuilder setPath(String path) {
            this.path = path;
            return this;
        }

        public WorkflowCreateFileBuilder setName(String name) {
            this.name = name;
            return this;
        }

        public WorkflowCreateFileBuilder then(String then) {
            this.then = then;
            return this;
        }

        public WorkflowInstanceBuilder and() {
            return parent;
        }
    }
}
