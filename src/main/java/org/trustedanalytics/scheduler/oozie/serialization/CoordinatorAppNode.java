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
import org.simpleframework.xml.Namespace;
import org.simpleframework.xml.Root;

import lombok.Data;

@Data
@Root(name = "coordinator-app")
@Namespace(reference = "uri:oozie:coordinator:0.4")
public class CoordinatorAppNode {

    @Attribute(name = "name")
    private String name;

    @Attribute(name = "frequency")
    private String frequency;

    @Attribute(name = "start")
    private String start;

    @Attribute(name = "end")
    private String end;

    @Attribute(name = "timezone")
    private String timezone;

    @Element(name = "action")
    private CoordinatorActionNode action;

}
