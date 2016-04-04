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

import org.simpleframework.xml.Element;
import org.simpleframework.xml.ElementList;
import org.simpleframework.xml.ElementListUnion;
import org.simpleframework.xml.Namespace;
import org.simpleframework.xml.Path;

import java.util.List;

import lombok.Data;

@Data
@Namespace(reference = "uri:oozie:sqoop-action:0.4")
public class SqoopActionNode implements WorkflowAction {

    @Element(name = "job-tracker")
    private String jobTracker;

    @Element(name = "name-node")
    private String nameNode;

    @Path("prepare")
    @ElementListUnion({
        @ElementList(entry = "delete", inline = true, type = DeleteNode.class),
        @ElementList(entry = "mkdir", inline = true, type = MkdirNode.class)
    })
    private List<FsAction> prepare;

    @Path("configuration")
    @ElementList(inline = true, entry = "property")
    private List<PropertyNode> configuration;

    @Element(required = false)
    private String command;

    @Element(required = false)
    private List<String> args;

}
