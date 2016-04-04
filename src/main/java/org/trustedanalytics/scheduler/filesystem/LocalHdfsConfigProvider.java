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
package org.trustedanalytics.scheduler.filesystem;

import org.apache.hadoop.conf.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

import java.util.UUID;

@Component
@Profile("local")
public class LocalHdfsConfigProvider implements HdfsConfigProvider{
    @Override
    public boolean isKerberosEnabled() {
        return false;
    }

    @Override
    public String getHdfsUri() {
        return "test_namenode";
    }

    @Override
    public String getKdc() {
        return null;
    }

    @Override
    public String getRealm() {
        return null;
    }

    @Override
    public Configuration getHadoopConf() {
        return null;
    }

    @Override
    public String getHdfsOrgUri(UUID org) {
        return null;
    }
}
