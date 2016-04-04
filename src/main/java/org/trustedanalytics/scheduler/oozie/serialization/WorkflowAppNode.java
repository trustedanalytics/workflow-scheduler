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
import org.simpleframework.xml.ElementList;
import org.simpleframework.xml.Namespace;
import org.simpleframework.xml.Root;

import java.util.List;

import lombok.Data;

@Data
@Root(name = "workflow-app")
@Namespace(reference = "uri:oozie:workflow:0.4")
public class WorkflowAppNode {

    @Element(name = "start")
    private StartNode start;

    @Element(name = "decision", required = false)
    private DecisionNode decision;

    @ElementList(inline = true, entry = "action")
    private List<WorkflowActionNode> workflowActions;

    @Element(name = "kill")
    private KillNode kill;

    @Element(name = "end")
    private EndNode end;

    @Attribute(name = "name")
    private final String name;
}
