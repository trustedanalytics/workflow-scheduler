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

import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;
import org.trustedanalytics.scheduler.filesystem.LocalHdfsConfigProvider;
import org.trustedanalytics.scheduler.rest.RestOperationsFactory;

import java.io.IOException;

@Component
public class MockRestOperationsFactory extends RestOperationsFactory {

    public MockRestOperationsFactory() throws IOException {
        super(new LocalHdfsConfigProvider());
    }

    public RestTemplate getRestTemplate()  {
        return new MockRestTemplate();
    }
}
