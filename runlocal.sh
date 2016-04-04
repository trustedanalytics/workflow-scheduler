#!/bin/bash

export JOB_TRACKER="jobTracker"
export OOZIE_API_URL="http://example.com:11000"
mvn spring-boot:run -Dspring.profiles.active=local -Dserver.port=8080

