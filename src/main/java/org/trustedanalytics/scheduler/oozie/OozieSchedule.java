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
package org.trustedanalytics.scheduler.oozie;

import java.time.LocalDateTime;
import java.time.ZoneId;

import com.fasterxml.jackson.annotation.JsonProperty;

import lombok.Data;

@Data
public class OozieSchedule {

    protected ZoneId zone;

    protected OozieFrequency frequency;

    protected LocalDateTime startTimeUtc;

    protected LocalDateTime endTimeUtc;

    public OozieSchedule(@JsonProperty("start") LocalDateTime start, @JsonProperty("end") LocalDateTime end, @JsonProperty("zoneId") ZoneId zoneId) {
        zone = zoneId;
        startTimeUtc = getUtcTimeFromLocal(start);
        endTimeUtc = getUtcTimeFromLocal(end);
    }

    public String coordinatorStart() {

        return startTimeUtc.toString() + "Z";
    }

    public String coordinatorEnd() {
        return endTimeUtc.toString() + "Z";
    }

    public String coordinatorZone() {
        return zone.toString();
    }

    private LocalDateTime getUtcTimeFromLocal(LocalDateTime localDateTime) {
        return localDateTime.minusSeconds(localDateTime.atZone(zone).getOffset().getTotalSeconds());
    }
}
