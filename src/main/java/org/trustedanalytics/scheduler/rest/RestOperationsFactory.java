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
package org.trustedanalytics.scheduler.rest;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.security.kerberos.client.KerberosRestTemplate;
import org.springframework.security.oauth2.provider.OAuth2Authentication;
import org.springframework.security.oauth2.provider.authentication.OAuth2AuthenticationDetails;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;
import org.trustedanalytics.hadoop.config.client.oauth.TapOauthToken;
import org.trustedanalytics.hadoop.kerberos.KrbLoginManager;
import org.trustedanalytics.hadoop.kerberos.KrbLoginManagerFactory;
import org.trustedanalytics.scheduler.filesystem.HdfsConfigProvider;
import sun.security.krb5.PrincipalName;

import javax.security.auth.login.LoginException;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

@Component
public class RestOperationsFactory {

    private static final Logger LOGGER = LoggerFactory.getLogger(RestOperationsFactory.class);

    private HdfsConfigProvider hdfsConfigProvider;

    @Autowired
    public RestOperationsFactory(HdfsConfigProvider configProvider) throws IOException {
        hdfsConfigProvider = configProvider;
    }

    private static final String KRB5_CREDENTIALS_CACHE_DIR = "/tmp/";

    static String ticketCacheLocation(String princName) {
        return KRB5_CREDENTIALS_CACHE_DIR
                + princName.replace(PrincipalName.NAME_COMPONENT_SEPARATOR, '_');
    }
    private static String getOAuthToken() {
        Authentication auth = SecurityContextHolder.getContext().getAuthentication();
        OAuth2Authentication oauth2 = (OAuth2Authentication) auth;
        OAuth2AuthenticationDetails details = (OAuth2AuthenticationDetails) oauth2.getDetails();
        return details.getTokenValue();
    }

    public RestTemplate getRestTemplate()  {
        if (hdfsConfigProvider.isKerberosEnabled()) {
            try {
                KrbLoginManager loginManager =
                        KrbLoginManagerFactory.getInstance().getKrbLoginManagerInstance(hdfsConfigProvider.getKdc(), hdfsConfigProvider.getRealm());
                TapOauthToken jwtToken = new TapOauthToken(getOAuthToken());
                loginManager.loginWithJWTtoken(jwtToken);
                Map<String, Object> options = new HashMap<>();

                options.put("ticketCache", ticketCacheLocation(jwtToken.getUserId() + PrincipalName.NAME_REALM_SEPARATOR_STR
                        + System.getProperty(HdfsConfigProvider.KRB5_REALM)));
                return new KerberosRestTemplate("", jwtToken.getUserId(), options);
            } catch (LoginException e) {
                LOGGER.error("Kerberos login exception", e);
                throw new IllegalStateException("Unable to authenticate in kerberos");
            }
        } else {
            LOGGER.warn("No valid Kerberos configuration detected, creating standard RestTemplate");
            return new RestTemplate();
        }
    }

}
