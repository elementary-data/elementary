# Integration contribution guide

> **DEPRECATED**: This guide describes the legacy integration system. For new integrations, please refer to the [Messaging Integrations Guide](../../messages/messaging_integrations/README.md) which describes the new `BaseMessagingIntegration` system.

The content below is kept for reference while existing integrations are migrated to the new system.

First of all, thank you for contributing a new integration to Elementary! We appreciate it :)
This guide is meant to make this process easier for you and make the review process faster.

Our integrations support either sending alerts, sending reports or both.
Basically adding a new integration consists of adding the authentication params, implementing a client and formatting and sending the alerts.

### CLI command and params

The authentication params need to be added to the `edr monitor` CLI command params and passed on to the integration class.
You can copy the behavior from the `slack_channel` and `slack_token` params.

In order to support sending reports to this destination as well,
please make sure those params are also available in the `edr send-report` CLI command.

### Integration Client

The client is the class that takes care of the authentication and all API calls that are required by the integration (listing channels, sending messages, etc.).
Create the client class under the `elementary/clients` folder.
The capabilities that should usually be supported in the client are:

1. Authentication - usually in the `__init__` method.
2. Listing channels / destinations / etc. - This is required to validate the destination exists and is accessible.
3. Sending messages / triggering incidents - This is the main functionality of the client, and it will be used by the alert integration to send the alert.
4. Sending files - This is required to support sending reports to this destination (to support `edr send report`).
5. Error handling - This is required to handle authentication errors and other errors that might occur during the API calls.
6. Searching users - This is required in messaging services in order to tag owners and subscribers in alerts

### Formatting and sending alerts

The integration class should be created under `elementary/monitor/data_monitoring/alerts/integrations/<integration_name>` folder and inherit from `BaseIntegration` (See slack.py for example).
This class is responsible for formatting the alert and sending it to the integration, using the client class that was implemented earlier.

_Note: The method `Integrations.get_integration` determines which alert integration to use based on the params passed in the CLI command. Please make sure to add support for the new integration in this method._

#### Sending alerts

The `send_alert` method gets the formatted alert and sends it to the integration.
The return value of the `send_alert` method is a boolean that indicates whether the alert was sent successfully or not.

#### Formatting alerts

This is the part that formats the alert object into what the integrated service requires.
There is a method for each type of alert that gets the alert object and returns the formatted alert.
The reason for this is that we might want to send different information for each alert type.
For example, a model alert will contain the model runtime error message, while a test alert will contain the test result message, query and test configuration.
You can use the Slack alerts as reference and consult with us to decide how to format the alerts, but in here you will find a list of the information that should be included in each alert type.
Please also take into consideration that:

1. Owners and subscribers should be tagged in the alert. This usually requires an API call to the integration to get the user ID.
2. The `Alert` object contains the `alert_fields` field, that is a list of fields configured by the user to be included in the alert. This list should be used in conjunction with the information required by each alert type to decide which fields to include in the final formattedalert.

The different alert types are:

##### 1. Elementary (anomaly / schema) test alerts (_method name:_ `_get_elementary_test_template`)

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

##### 2. DBT test alerts (_method name:_ `_get_dbt_test_template`)

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

##### 3. Model / Snapshot error alerts (_method name:_ `_get_model_template` or `_get_snapshot_template`)

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

##### 4. DBT source freshness alerts (_method name:_ `_get_source_freshness_template`)

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

##### 5. Alerts grouped by table (_method name:_ `_get_group_by_table_template`)

- link to report
- Table
- List of failures
- List of warnings
- Tags
- Owners
- Subscribers

#### 6. Fallback template (_method name:_ `_get_fallback_template`)

We try to send the formatted message and in case it fails (due to a bug or API change) we send the fallback alert, which is usually a raw JSON of the alert object.
You can find an example of this in the Slack integration (`elementary/monitor/data_monitoring/alerts/integrations/slack.py`).
