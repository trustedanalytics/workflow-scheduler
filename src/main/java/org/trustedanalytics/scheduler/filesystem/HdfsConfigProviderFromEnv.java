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

import com.google.common.collect.ImmutableMap;
import lombok.Getter;
import org.apache.commons.lang.text.StrSubstitutor;
import org.apache.hadoop.conf.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;
import org.trustedanalytics.hadoop.config.client.*;

import java.io.IOException;
import java.util.UUID;

@Component
@Profile("cloud")
public class HdfsConfigProviderFromEnv implements HdfsConfigProvider {

    private static final String AUTHENTICATION_METHOD = "kerberos";
    private static final String AUTHENTICATION_METHOD_PROPERTY = "hadoop.security.authentication";

    @Getter
    private final ServiceInstanceConfiguration hdfsConf;
    @Getter
    private ServiceInstanceConfiguration krbConf;
    @Getter
    private String kdc;
    @Getter
    private String realm;
    @Getter
    private final Configuration hadoopConf;

    public HdfsConfigProviderFromEnv() throws IOException {
        AppConfiguration appConfiguration = Configurations.newInstanceFromEnv();
        hdfsConf = appConfiguration.getServiceConfig(ServiceType.HDFS_TYPE);
        krbConf = appConfiguration.getServiceConfig(ServiceType.KERBEROS_TYPE);
        kdc = krbConf.getProperty(Property.KRB_KDC).get();
        realm = krbConf.getProperty(Property.KRB_REALM).get();
        hadoopConf = hdfsConf.asHadoopConfiguration();
    }

    @Override
    public boolean isKerberosEnabled() {
        return AUTHENTICATION_METHOD.equals(hadoopConf.get(AUTHENTICATION_METHOD_PROPERTY));
    }

    @Override
    public String getHdfsUri() {
        return hadoopConf.get("fs.defaultFS");
    }

    @Override
    public String getHdfsOrgUri(UUID org) {
        return PathTemplate.resolveOrg(hdfsConf.getProperty(Property.HDFS_URI).get(), org);
    }

    private static class PathTemplate {
        private static final String ORG_PLACEHOLDER = "organization";
        private static final String PLACEHOLDER_PREFIX = "%{";
        private static final String PLACEHOLDER_SUFIX = "}";

        private PathTemplate() {
        }

        private static String resolveOrg(String url, UUID org) {
            ImmutableMap<String, UUID> values = ImmutableMap.of(ORG_PLACEHOLDER, org);
            return new StrSubstitutor(values, PLACEHOLDER_PREFIX, PLACEHOLDER_SUFIX).replace(url);
        }
    }

}
