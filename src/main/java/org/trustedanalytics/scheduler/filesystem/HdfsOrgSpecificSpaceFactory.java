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

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.trustedanalytics.scheduler.security.TokenProvider;

import java.io.IOException;
import java.util.Objects;
import java.util.UUID;

@Component
public class HdfsOrgSpecificSpaceFactory implements OrgSpecificSpaceFactory {

    private final FileSystemFactory fileSystemFactory;
    private final TokenProvider tokenProvider;

    @Autowired
    public HdfsOrgSpecificSpaceFactory(FileSystemFactory fileSystemFactory, TokenProvider tokenProvider) {
        this.fileSystemFactory = fileSystemFactory;
        this.tokenProvider = tokenProvider;
    }

    @Override
    public OrgSpecificSpace getOrgSpecificSpace(UUID orgID) throws IOException {

        Objects.requireNonNull(orgID, "Organization identifier is required");
        return new HdfsOrgSpecificSpace(fileSystemFactory.getFileSystem(orgID), orgID, tokenProvider);
    }
}


