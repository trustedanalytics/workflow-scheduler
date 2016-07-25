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

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;
import org.trustedanalytics.scheduler.oozie.OozieSchedule;

import java.time.LocalDateTime;
import java.time.ZoneId;

import static org.junit.Assert.assertTrue;

@RunWith(MockitoJUnitRunner.class)
public class OozieScheduleTest {


    @Test
    public void should_returnSameTime_when_timezoneIsUtc() {
        LocalDateTime start = LocalDateTime.of(2077,03,12, 15,20);
        LocalDateTime end = LocalDateTime.of(2077,04,12, 15,20);
        ZoneId zoneId = ZoneId.of("UTC");
        OozieSchedule schedule = new OozieSchedule(start, end, zoneId);
        assertTrue(start.equals(schedule.getStartTimeUtc()));
        assertTrue(end.equals(schedule.getEndTimeUtc()));
    }

    @Test
    public void should_goBackOneHour_when_timezoneIsGmtPlusOne() {
        LocalDateTime start = LocalDateTime.of(2077,03,12, 15,20);
        LocalDateTime end = LocalDateTime.of(2077,04,12, 15,20);
        ZoneId zoneId = ZoneId.of("GMT+1");
        OozieSchedule schedule = new OozieSchedule(start, end, zoneId);
        assertTrue(start.minusHours(1).equals(schedule.getStartTimeUtc()));
        assertTrue(end.minusHours(1).equals(schedule.getEndTimeUtc()));
    }

    @Test
    public void should_goAheadOneHour_when_timezoneIsGmtMinusOne() {
        LocalDateTime start = LocalDateTime.of(2077,03,12, 15,20);
        LocalDateTime end = LocalDateTime.of(2077,04,12, 15,20);
        ZoneId zoneId = ZoneId.of("GMT-1");
        OozieSchedule schedule = new OozieSchedule(start, end, zoneId);
        assertTrue(start.plusHours(1).equals(schedule.getStartTimeUtc()));
        assertTrue(end.plusHours(1).equals(schedule.getEndTimeUtc()));
    }

    @Test
    public void should_goAheadSevenHours_when_timezoneIsLosAngeles() {
        LocalDateTime start = LocalDateTime.of(2077,04,12, 15,20);
        LocalDateTime end = LocalDateTime.of(2077,05,12, 15,20);
        ZoneId zoneId = ZoneId.of("America/Los_Angeles");
        OozieSchedule schedule = new OozieSchedule(start, end, zoneId);
        assertTrue(start.plusHours(7).equals(schedule.getStartTimeUtc()));
        assertTrue(end.plusHours(7).equals(schedule.getEndTimeUtc()));
    }
}
