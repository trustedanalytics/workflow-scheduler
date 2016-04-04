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

@Data
@JsonIgnoreProperties(ignoreUnknown = true)
public class OozieWorkflowJobAction {
    private String status;
    private String externalStatus;
    private String endTime;
    private String id;
    private String toString;
    private String startTime;
    private String name;
    private String externalId;
    private String externalChildIDs;
    private Integer userRetryCount;
    private String errorMessage;
    private String data;
    private String trackerUri;
    private Integer userRetryMax;
    private String cred;
    private String errorCode;
    private String conf;
    private String type;
    private String transition;
    private Integer retries;
    private String consoleUrl;
    private String stats;
    private Integer userRetryInterval;
}
