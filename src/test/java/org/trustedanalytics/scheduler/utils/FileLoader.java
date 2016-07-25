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

import org.apache.commons.io.IOUtils;

import java.io.IOException;

public class FileLoader {
    public static String readFileResource(String name)  {
        String fileContent = null;
        try {
            fileContent = IOUtils.toString(
                    FileLoader.class.getResourceAsStream(name),
                    "UTF-8"
            );
        } catch (IOException e) {
            throw new IllegalStateException(String.format("Could not read file resource (%s)", name));
        }
        return fileContent;
    }

    public static String readFileResourceNormalized(String name)  {
        String fileContent = null;
        try {
            fileContent = IOUtils.toString(
                    FileLoader.class.getResourceAsStream(name),
                    "UTF-8"
            );
        } catch (IOException e) {
            throw new IllegalStateException(String.format("Could not read file resource normalized (%s)", name));
        }
        return fileContent.replaceAll("[ \t\r]","").trim();
    }
}
