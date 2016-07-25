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
package org.trustedanalytics.scheduler.utils;

import lombok.Getter;

import org.springframework.http.HttpEntity;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.RestClientException;
import org.springframework.web.client.RestTemplate;
import org.trustedanalytics.scheduler.client.OozieJobId;

import java.net.URI;
import java.util.Map;

// no thread safe, for unit tests only
public class MockRestTemplate extends RestTemplate {

    @Getter
    private static String requestBody;

    @Override
    public <T> ResponseEntity<T> postForEntity(String url, Object request, Class<T> responseType, Object... uriVariables) throws RestClientException {
        HttpEntity<String> entity = (HttpEntity<String>) request;
        requestBody = entity.getBody();
        return new ResponseEntity<>((T)new OozieJobId(), null, HttpStatus.OK);
    }

    @Override
    public <T> ResponseEntity<T> postForEntity(String url, Object request, Class<T> responseType, Map<String, ?> uriVariables) throws RestClientException {
        HttpEntity<String> entity = (HttpEntity<String>) request;
        requestBody = entity.getBody();
        return new ResponseEntity<>((T)new OozieJobId(), null, HttpStatus.OK);
    }

    @Override
    public <T> ResponseEntity<T> postForEntity(URI url, Object request, Class<T> responseType) throws RestClientException {
        HttpEntity<String> entity = (HttpEntity<String>) request;
        requestBody = entity.getBody();
        return new ResponseEntity<>((T) new OozieJobId(), null, HttpStatus.OK);
    }
}
