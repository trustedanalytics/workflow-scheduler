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
package org.trustedanalytics.scheduler.oozie.jobs;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public abstract class AbstractCommandLine implements CommandLine {

    private static final String EMPTY = "";
    private final Map<String, String> arguments = new LinkedHashMap<>();

    @Override
    public String command() {
        return name() + " " + String.join(" ", arguments());
    }

    @Override
    public List<String> arguments() {
        return arguments.entrySet()
                        .stream()
                        .flatMap(entry -> Stream.of(entry.getKey(), entry.getValue()))
                        .filter(argument -> !EMPTY.equals(argument))
                        .collect(Collectors.toList());
    }

    protected void requiredArgument(String flag) {
        Objects.requireNonNull(flag);

        arguments.put(flag, EMPTY);
    }

    protected void requiredArgument(String key, String value) {
        Objects.requireNonNull(key);
        Objects.requireNonNull(value, String.format("%s is required", key));

        arguments.put(key, value);
    }

    protected void optionalArgument(String flag) {
        if (flag != null) {
            arguments.put(flag, EMPTY);
        }
    }

    protected void optionalArgument(String key, String value) {
        if ((key != null) && (value != null)) {
            arguments.put(key, value);
        }
    }

    protected void optionalStringArgument(String key, String value, Boolean isEnabled) {
        if((isEnabled != null) && isEnabled) {
            if ((key != null) && (value != null)) {
                arguments.put(key, value);
            }
        }

    }

    protected void optionalArgument(String flag, Boolean isPresent) {
        if((isPresent != null) && isPresent) {
            requiredArgument(flag);
        }
    }

}
