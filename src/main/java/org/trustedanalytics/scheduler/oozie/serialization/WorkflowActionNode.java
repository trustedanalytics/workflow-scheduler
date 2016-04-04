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

import org.simpleframework.xml.Attribute;
import org.simpleframework.xml.Element;
import org.simpleframework.xml.ElementUnion;

import lombok.Data;

@Data
public class WorkflowActionNode {

    @Attribute
    private final String name;

    @ElementUnion({
        @Element(name = "sqoop", type = SqoopActionNode.class),
        @Element(name = "fs", type = FsActionNode.class)
    })
    private WorkflowAction action;

    @Element(name = "ok")
    private OkNode ok;

    @Element(name = "error")
    private ErrorNode error;
}
