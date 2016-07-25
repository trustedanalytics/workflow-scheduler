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
package org.trustedanalytics.scheduler.oozie.jobs.sqoop;

import java.util.List;

import lombok.Data;

@Data
public class SqoopImport {
    // Specify JDBC connect string
    private String jdbcUri;

    // 	Manually specify JDBC driver class to use
    private String driver;

    // 	Set authentication password
    private String username;

    // 	Set authentication username
    private String password;

    // Print more information while working
    private Boolean verbose;

    // Append data to an existing directory on HDFS
    private Boolean append = true;

    // Columns to import from table
    private List<String> columns;

    // Use n map tasks to import in parallel
    private Integer mappers;

    // Table to read
    private String table;

    // HDFS destination dir
    private String targetDir;

    // Enable compression
    private Boolean compress;

    // Cleans output dir
    private Boolean overwrite = false;

    // The string to be written for a null value for string columns
    private String nullString;

    // The string to be written for a null value for non-string columns
    private String nullNonString;

    // Specifies the column to be examined when determining which rows to import.
    private String checkColumn;

    // Specifies the maximum value of the check column from the previous import.
    private String lastValue = String.valueOf(0);

    // Specifies how Sqoop determines which rows are new.
    private Boolean incremental;

    private String importMode;

    private String schema;
}
