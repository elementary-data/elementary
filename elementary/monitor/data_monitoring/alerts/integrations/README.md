# Integration contribution guide

First of all, thank you for contributing a new integration to Elementary! We appreciate it :)
This guide is meant to make this process easier for you and make the review process faster.

Our integrations support either sending alerts, sending reports or both.
Basically adding a new integration consists of adding the authentication params, implementing a client and formatting and sending the alerts.

### CLI command and params

The authentication params need to be added to the `edr monitor` CLI command params and passed on to the integration class.
You can copy the behavior from the slack_channel and slack_token params.
If you'd like to support sending reports to this destination as well,
please also add those params to the `edr send-report` CLI command.

### Client

Create the client class under the `clients` folder, the class should take care of the authentication and implement a method that sends messages.
Then, in the integration class, implement the `send_alert` method, which gets the formatted alert and sends it to the integration.
The return value of the `send_alert` method is a boolean that indicates whether the alert was sent successfully or not.

To support sending reports in this integration, a method that sends files also needs to be implemented,
along with the required logic in the `DataMonitoringReport.send_report` method.

### Alert formatting

This is the part that formats the alert object into a serializable object that can be sent to the integration.
There are a few types of alerts, and each has different information and formatting,
so there is a method for each type of alert that gets the alert object and returns the formatted alert:

##### 1. _method name:_ `_get_elementary_test_template`

- elementary test name
- last run time
- link to report
- status (fail / warn / error)
- Table
- Column
- Tags
- Owners
- Subscribers
- Description
- Result message
- Test configuration

##### 2. _method name:_ `_get_dbt_test_template`

- elementary test name
- last run time
- link to report
- status (fail / warn / error)
- Table
- Column
- Tags
- Owners
- Subscribers
- Description
- Result message
- Result sample
- Test query
- Test configuration

##### 3. _method name:_ `_get_dbt_model_template` or `_get_dbt_snapshot_template`

- model name
- Last run time
- Link to report
- Status (warn / error)
- Tags
- Owners
- Subscribers
- Description
- Error message
- Configuration (materialization + path)

##### _method name:_ `_get_dbt_source_freshness_template`

- source name
- last run time
- link to report
- status (warn / error)
- Tags
- Owners
- Subscribers
- Description
- Result message
- Time elapsed, last record timestamp
- Configuration

##### _method name:_ `_get_group_by_table_template`

- link to report
- Table
- List of failures
- List of warnings
- Tags
- Owners
- Subscribers
