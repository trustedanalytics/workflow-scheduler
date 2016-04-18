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
import com.google.common.base.Strings;
import lombok.AccessLevel;
import lombok.Data;
import lombok.Getter;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Data
@JsonIgnoreProperties(ignoreUnknown = true)
public class OozieWorkflowJobInformationExtended extends OozieWorkflowJobInformation {

    private String coordinatorId;
    private List<String> targetDirs;

    @Getter(AccessLevel.PRIVATE)
    private final Pattern patter = Pattern.compile("targetDir<\\/name>[\\s]*<value>([^<]*)<\\/value>");

    @Getter(AccessLevel.PRIVATE)
    private final String dirPrefix;

    public OozieWorkflowJobInformationExtended(OozieWorkflowJobInformation oozieWorkflowJobInformation, String dirPrefix) {

        group = oozieWorkflowJobInformation.group;
        status = oozieWorkflowJobInformation.status;
        lastModTime = oozieWorkflowJobInformation.lastModTime;
        parentId = oozieWorkflowJobInformation.parentId;
        appPath = oozieWorkflowJobInformation.appPath;
        acl = oozieWorkflowJobInformation.acl;
        run = oozieWorkflowJobInformation.run;
        conf = oozieWorkflowJobInformation.conf;
        externalId = oozieWorkflowJobInformation.externalId;
        appName = oozieWorkflowJobInformation.appName;
        consoleUrl = oozieWorkflowJobInformation.consoleUrl;
        createdTime = oozieWorkflowJobInformation.createdTime;
        startTime = oozieWorkflowJobInformation.startTime;
        toString = oozieWorkflowJobInformation.toString;
        id = oozieWorkflowJobInformation.id;
        endTime = oozieWorkflowJobInformation.endTime;
        user = oozieWorkflowJobInformation.user;
        actions = oozieWorkflowJobInformation.actions;
        this.dirPrefix = dirPrefix;
        this.coordinatorId = getCoordinatorIdFromParentId(oozieWorkflowJobInformation.getParentId());
        this.targetDirs = extractTargetIdsFromConfiguration(conf);
    }

    private String getCoordinatorIdFromParentId(String parentId) {
        if(!Strings.isNullOrEmpty(parentId)) {
            return parentId.substring(0, parentId.indexOf('@'));
        }
        return null;
    }

    private List<String> extractTargetIdsFromConfiguration(String conf) {
        List<String> extractedTargetDirs= new ArrayList<>();
        if (StringUtils.isEmpty(conf))
            return extractedTargetDirs;
        final Matcher matcher = patter.matcher(conf);
        if(matcher.find() && matcher.groupCount() > 0) {
            extractedTargetDirs.add(matcher.group(1));
        }
        return extractedTargetDirs;
    }
}
