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
import org.apache.htrace.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

@Data
@JsonIgnoreProperties(ignoreUnknown = true)
public class OozieCoordinatedJobInformation {

    private String status;
    private List<OozieCoordinatedJobAction> actions;
    private String user;
    private String endTime;
    private String toString;
    private String nextMaterializedTime;
    private Integer timeOut;
    private Integer concurrency;
    private String timeZone;
    private String bundleId;
    private String consoleUrl;
    private String pauseTime;
    private Integer total;
    private String frequency;
    private String acl;
    private String conf;
    private String executionPolicy;
    private String lastAction;
    private String coordJobName;
    private String startTime;
    private String coordExternalId;
    private String timeUnit;
    private String group;
    private String coordJobId;
    private String coordJobPath;

    @JsonProperty("mat_throttling")
    private Integer matThrottling;
}
