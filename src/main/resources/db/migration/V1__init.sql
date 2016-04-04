CREATE TABLE if NOT EXISTS oozie_job_entity (job_id VARCHAR(255), org_id VARCHAR(255) NOT NULL, PRIMARY KEY (job_id));

CREATE INDEX org_index
ON oozie_job_entity (org_id)