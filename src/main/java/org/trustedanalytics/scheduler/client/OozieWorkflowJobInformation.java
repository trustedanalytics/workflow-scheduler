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
package org.trustedanalytics.scheduler.client;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.Data;

import java.util.List;

@Data
//ToDo: Configure client and remove JsonIgnoreProperties
@JsonIgnoreProperties(ignoreUnknown = true)
public class OozieWorkflowJobInformation {

    protected String group;
    protected String status;
    protected String lastModTime;
    protected String parentId;
    protected String appPath;
    protected String acl;
    protected Integer run;
    protected String conf;
    protected String externalId;
    protected String appName;
    protected String consoleUrl;
    protected String createdTime;
    protected String startTime;
    protected String toString;
    protected String id;
    protected String endTime;
    protected String user;
    protected List<OozieWorkflowJobAction> actions;
}
