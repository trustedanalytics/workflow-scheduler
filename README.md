# workflow-scheduler
Job scheduling service

### Import data from a SQL database
* `name` - is a name of your job
* `jdbcUri` - required schema of jdbc uri: 
```
jdbc:driver://host:port/database_name
```
* `username` and `password` - if database is secured you need to pass credentials
* `table` - it's name of table from database specified for import
* `destination/target dir` - it's directory where you will import data
* `importMode` - there are 3 possible modes: `append`, `overwrite` and `incremental`
  * `append` - each import will fetch whole table into separate file. Results of previous imports will not be overwritten.
  * `overwrite` - each import will fetch whole table and overwrite results of previous import.
  * `incremental` - each import will fetch the difference from last import, and store it in separate file. Sqoop will recognize the delta by value of specific column, so column name containing id must be specified. We recommend using column which is auto-incremented. The initial value of column can be specified. Use 0 to import whole table during first run.
    * `checkColumn` - it's column from database, from which lastValue will be checked
    * `lastValue` - it's value from which you want import database
* `start` - it's start time of your job
* `end` - it's end time of your job
* `frequency` - it's frequency with which your job will be submitted
* `zoneId` - it's id of the time zone in which you entered start and end time
* Example of request body json:
```
{
	"name" : "job_name",
	"sqoopImport" : {
		"jdbcUri" : "jdbc:driver://host:port/database_name",
		"table" : "table_name",
		"username" : "user_name",
		"password" : "password",
		"targetDir" : "",
		"append" : true,
		"importMode" : "incremental",
		"checkColumn" : "id",
		"lastValue" : "0"
	},
	"schedule" : {
		"zoneId" : "UTC",
		"frequency" : {
			"unit" : "minutes",
			"amount" : 15
		},
		"start" : "03/29/2016 04:20 PM",
		"end" : "03/29/2016 05:55 PM"
	}
}
```