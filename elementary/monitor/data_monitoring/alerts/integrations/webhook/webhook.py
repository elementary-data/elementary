import json
import simplejson
import requests
from datetime import datetime, timedelta
from typing import Any, Dict, Optional, Union
from requests.auth import HTTPBasicAuth
from oauthlib.oauth2 import BackendApplicationClient
from requests_oauthlib import OAuth2Session
import pandas as pd
from pymsteams import cardsection, potentialaction


from elementary.config.config import Config
from elementary.monitor.alerts.alerts_groups import AlertsGroup, GroupedByTableAlerts, BaseAlertsGroup
from elementary.monitor.alerts.model_alert import ModelAlertModel
from elementary.monitor.alerts.source_freshness_alert import SourceFreshnessAlertModel
from elementary.monitor.alerts.test_alert import TestAlertModel
from elementary.monitor.data_monitoring.alerts.integrations.base_integration import (
    BaseIntegration,
)

from elementary.utils.json_utils import (
    list_of_lists_of_strings_to_comma_delimited_unique_strings,
)
from elementary.tracking.tracking_interface import Tracking
from elementary.utils.log import get_logger
from elementary.utils.strings import prettify_and_dedup_list
from jinja2 import Environment, FileSystemLoader, PackageLoader


logger = get_logger(__name__)

TABLE_FIELD = "table"
COLUMN_FIELD = "column"
DESCRIPTION_FIELD = "description"
OWNERS_FIELD = "owners"
TAGS_FIELD = "tags"
SUBSCRIBERS_FIELD = "subscribers"
RESULT_MESSAGE_FIELD = "result_message"
TEST_PARAMS_FIELD = "test_parameters"
TEST_QUERY_FIELD = "test_query"
TEST_RESULTS_SAMPLE_FIELD = "test_results_sample"
DEFAULT_ALERT_FIELDS = [
    TABLE_FIELD,
    COLUMN_FIELD,
    DESCRIPTION_FIELD,
    OWNERS_FIELD,
    TAGS_FIELD,
    SUBSCRIBERS_FIELD,
    RESULT_MESSAGE_FIELD,
    TEST_PARAMS_FIELD,
    TEST_QUERY_FIELD,
    TEST_RESULTS_SAMPLE_FIELD,
]

STATUS_DISPLAYS: Dict[str, Dict] = {
    "fail": {"display_name": "Failure"},
    "warn": {"display_name": "Warning"},
    "error": {"display_name": "Error"},
}


class WebhookIntegration(BaseIntegration):
    def __init__(
        self,
        config: Config,
        tracking: Optional[Tracking] = None,
        override_config_defaults=False,
        *args,
        **kwargs,
    ) -> None:
        self.config = config
        self.tracking = tracking
        self.override_config_defaults = override_config_defaults
        super().__init__()


    def _get_alert_template(
        self,
        alert: Union[
            TestAlertModel,
            ModelAlertModel,
            SourceFreshnessAlertModel,
            GroupedByTableAlerts,
            BaseAlertsGroup,
        ],
        template=None,
        integration_params=None,
        *args,
        **kwargs,
    ):
        if isinstance(alert, TestAlertModel):
            if alert.is_elementary_test:
                return self._get_elementary_test_template(alert, template=template, integration_params=integration_params)
            else:
                return self._get_dbt_test_template(alert, template=template, integration_params=integration_params)
        elif isinstance(alert, ModelAlertModel):
            if alert.materialization == "snapshot":
                return self._get_snapshot_template(alert, template=template, integration_params=integration_params)
            else:
                return self._get_model_template(alert, template=template, integration_params=integration_params)
        elif isinstance(alert, SourceFreshnessAlertModel):
            return self._get_source_freshness_template(alert, template=template, integration_params=integration_params)
        elif isinstance(alert, GroupedByTableAlerts):
            return self._get_group_by_table_template(alert, template=template, integration_params=integration_params)
        elif isinstance(alert, BaseAlertsGroup):
            return self._get_alerts_group_template(alert, template=template, integration_params=integration_params)

    @staticmethod
    def _get_alert_sub_title(
        alert: Union[
            TestAlertModel,
            ModelAlertModel,
            SourceFreshnessAlertModel,
        ],
    ) -> str:
        subtitle = "**"
        subtitle += f"Status: {alert.status}"
        if alert.suppression_interval:
            subtitle += f"   |   Time: {alert.detected_at_str}"
            subtitle += (
                f"   |   Suppression interval: {alert.suppression_interval} hours"
            )
        else:
            subtitle += f"   |   {alert.detected_at_str}"
        subtitle += "**"

        return subtitle

    @staticmethod
    def _get_display_name(alert_status: Optional[str]) -> str:
        if alert_status is None:
            return "Unknown"
        return STATUS_DISPLAYS.get(alert_status, {}).get("display_name", alert_status)

    def _get_report_link_if_applicable(
            self,
            alert: Union[
                TestAlertModel,
                ModelAlertModel,
                SourceFreshnessAlertModel,
                GroupedByTableAlerts,
            ],
    ):
        report_link = alert.get_report_link()
        if report_link:
            return report_link
        return None

    def _get_table_field_if_applicable(self, alert: TestAlertModel):
        if TABLE_FIELD in (alert.alert_fields or DEFAULT_ALERT_FIELDS):
            return f"_{alert.table_full_name}_"
        return None

    def _get_column_field_if_applicable(self, alert: TestAlertModel):
        if COLUMN_FIELD in (alert.alert_fields or DEFAULT_ALERT_FIELDS):
            return  f'_{alert.column_name or "No column"}_'
        return None

    def _get_tags_field_if_applicable(
            self,
            alert: Union[
                TestAlertModel,
                ModelAlertModel,
                SourceFreshnessAlertModel,
            ],
    ):
        if TAGS_FIELD in (alert.alert_fields or DEFAULT_ALERT_FIELDS):
            tags = prettify_and_dedup_list(alert.tags or [])
            return  f'_{tags or "No tags"}_'
        return None

    def _get_owners_field_if_applicable(
            self,
            alert: Union[
                TestAlertModel,
                ModelAlertModel,
                SourceFreshnessAlertModel,
            ],
    ):
        if OWNERS_FIELD in (alert.alert_fields or DEFAULT_ALERT_FIELDS):
            owners = prettify_and_dedup_list(alert.owners or [])
            return  f'_{owners or "No owners"}_'
        return None

    def _get_subscribers_field_if_applicable(
            self,
            alert: Union[
                TestAlertModel,
                ModelAlertModel,
                SourceFreshnessAlertModel,
            ],
    ):
        if SUBSCRIBERS_FIELD in (alert.alert_fields or DEFAULT_ALERT_FIELDS):
            subscribers = prettify_and_dedup_list(alert.subscribers or [])
            return  f'_{subscribers or "No subscribers"}_'
        return None

    def _get_description_field_if_applicable(self, alert: TestAlertModel):
        if DESCRIPTION_FIELD in (alert.alert_fields or DEFAULT_ALERT_FIELDS):
           return f'_{alert.test_description or "No description"}_'
        return None

    def _get_result_message_field_if_applicable(
            self,
            alert: Union[
                TestAlertModel,
                ModelAlertModel,
            ],
    ):
        message = None
        if RESULT_MESSAGE_FIELD in (alert.alert_fields or DEFAULT_ALERT_FIELDS):
            if isinstance(alert, ModelAlertModel):
                if alert.message:
                    message = alert.message.strip()
            elif isinstance(alert, TestAlertModel):
                if alert.error_message:
                    message = alert.error_message.strip()
            if not message:
                message = "No result message"
            return  f"_{message}_"
        return None

    def _get_test_query_field_if_applicable(self, alert: TestAlertModel):
        # This lacks logic to handle the case where the message is too long
        if (
                TEST_QUERY_FIELD in (alert.alert_fields or DEFAULT_ALERT_FIELDS)
                and alert.test_results_query
        ):
            return f"```{alert.test_results_query.strip()}"
        return None

    def _get_test_params_field_if_applicable(self, alert: TestAlertModel):
        if (
                TEST_PARAMS_FIELD in (alert.alert_fields or DEFAULT_ALERT_FIELDS)
                and alert.test_params
        ):
            return "*Test parameters*", f"```{alert.test_params}```"
        return None

    def _get_test_results_sample_field_if_applicable(
            self, alert: TestAlertModel
    ):
        if TEST_RESULTS_SAMPLE_FIELD in (
                alert.alert_fields or DEFAULT_ALERT_FIELDS
        ) and alert.test_rows_sample is not None and len(alert.test_rows_sample) > 0:
            df = pd.DataFrame(alert.test_rows_sample)
            return df.to_string(index=False)
        return None

    def _get_test_anomalous_value_if_applicable(
            self, alert: TestAlertModel
    ):
        if TEST_RESULTS_SAMPLE_FIELD in (
                alert.alert_fields or DEFAULT_ALERT_FIELDS
        ) and  alert.test_type == "anomaly_detection" :
            anomalous_value = alert.other
            if alert.column_name:
                return f"Column: {alert.column_name}     |     Anomalous value: {anomalous_value}"
            else:
                return f"Anomalous value: {anomalous_value}"
        return None

    def _get_recipients(self,alert: Union[
                TestAlertModel,
                ModelAlertModel,
                SourceFreshnessAlertModel,
                AlertsGroup
            ]):
        recipients = []
        if isinstance(alert, AlertsGroup):
            for alert in alert.alerts:
                if alert.owners is not None:
                    recipients = recipients + alert.owners
                if alert.subscribers is not None:
                    recipients = recipients + alert.subscribers
        else:
            if alert.owners is not None:
                recipients = recipients + alert.owners
            if alert.subscribers is not None:
                recipients = recipients + alert.subscribers
        return recipients

    def _initial_client(self, *args, **kwargs):
        pass

    def _get_dbt_test_template(self, alert: TestAlertModel, template=None, integration_params: Dict=None, *args, **kwargs):
        title = f"{self._get_display_name(alert.status)}: {alert.summary}"
        subtitle = self._get_alert_sub_title(alert)
        report_link = self._get_report_link_if_applicable(alert)
        fields = [
            {"key":"Table", "value": self._get_table_field_if_applicable(alert)},
            {"key":"Column", "value": self._get_column_field_if_applicable(alert)},
            {"key":"Owners", "value": self._get_owners_field_if_applicable(alert)},
            {"key":"Subscribers", "value": self._get_subscribers_field_if_applicable(alert)},
            {"key":"Description", "value": self._get_description_field_if_applicable(alert)},
            {"key":"Result message", "value": self._get_result_message_field_if_applicable(alert)},
            {"key":"Test query", "value": self._get_test_query_field_if_applicable(alert)},
            {"key":"Test params", "value":  self._get_test_params_field_if_applicable(alert)}
        ]
        return template.render(alert=alert,
                               target_type=integration_params.get("target_type"),
                               target_channel=integration_params.get("target_channel"),
                               recipients=integration_params.get("recipients"),
                               included_html=integration_params.get("included_html"),
                               title=title,
                               subtitle=subtitle,
                               report_link=report_link,
                               fields=fields,
                               test_results_sample=self._get_test_results_sample_field_if_applicable(alert),

                               )

    def _get_elementary_test_template(self, alert: TestAlertModel, template=None, integration_params=None, *args, **kwargs):
        if alert.test_type == "schema_change":
            title = f"{alert.summary}"
        else:
            title = f"{self._get_display_name(alert.status)}: {alert.summary}"
        subtitle = self._get_alert_sub_title(alert)
        report_link = self._get_report_link_if_applicable(alert)

        anomalous_value = (
            alert.other if alert.test_type == "anomaly_detection" else None
        )

        fields = [
            {"key":"Table", "value": self._get_table_field_if_applicable(alert)},
            {"key":"Column", "value": self._get_column_field_if_applicable(alert)},
            {"key":"Owners", "value": self._get_owners_field_if_applicable(alert)},
            {"key":"Subscribers", "value": self._get_subscribers_field_if_applicable(alert)},
            {"key":"Description", "value": self._get_description_field_if_applicable(alert)},
            {"key":"Result message", "value": self._get_result_message_field_if_applicable(alert)},
            {"key":"Test query", "value": self._get_test_query_field_if_applicable(alert)},
            {"key":"Test params", "value": self._get_test_params_field_if_applicable(alert)}
        ]
        return template.render(alert=alert,
                               title=title,
                               subtitle=subtitle,
                               report_link=report_link,
                               fields=fields,
                               target_type=integration_params.get("target_type"),
                               target_channel=integration_params.get("target_channel"),
                               recipients=integration_params.get("recipients"),
                               included_html=integration_params.get("included_html"),
                               test_results_sample=self._get_test_results_sample_field_if_applicable(alert),
                               anomalous_value=self._get_test_anomalous_value_if_applicable(alert)
                               )


    def _get_model_template(self, alert: ModelAlertModel, template=None, integration_params=None, *args, **kwargs):
        title = f"{self._get_display_name(alert.status)}: {alert.summary}"
        subtitle = self._get_alert_sub_title(alert)
        report_link = self._get_report_link_if_applicable(alert)

        fields = [
            {"key":"Tags", "value": self._get_tags_field_if_applicable(alert)},
            {"key":"Owners", "value": self._get_owners_field_if_applicable(alert)},
            {"key":"Subscribers", "value": self._get_subscribers_field_if_applicable(alert)},
            {"key":"Result message", "value": self._get_result_message_field_if_applicable(alert)}
        ]

        if alert.materialization:
            fields.append({"key": "Materialisation", "value": f"`{str(alert.materialization)}`"})
        if alert.full_refresh:
            fields.append({"key":"Full refresh", "value": f"`{alert.full_refresh}`"})
        if alert.path:
            fields.append({"key": "Path", "value": f"`{alert.path}`"})

        return template.render(alert=alert,
                               title=title,
                               subtitle=subtitle,
                               report_link=report_link,
                               fields=fields,
                               target_type=integration_params.get("target_type"),
                               target_channel=integration_params.get("target_channel"),
                               recipients=integration_params.get("recipients"),
                               included_html=integration_params.get("included_html"),
                               )


    def _get_snapshot_template(self, alert: ModelAlertModel, template=None, integration_params=None, *args, **kwargs):
        title = f"{self._get_display_name(alert.status)}: {alert.summary}"
        subtitle = self._get_alert_sub_title(alert)
        report_link = self._get_report_link_if_applicable(alert)

        fields = [
            {"key":"Tags", "value": self._get_tags_field_if_applicable(alert)},
            {"key":"Owners", "value": self._get_owners_field_if_applicable(alert)},
            {"key":"Subscribers", "value": self._get_subscribers_field_if_applicable(alert)},
            {"key":"Result_message", "value": self._get_result_message_field_if_applicable(alert)}
        ]

        if alert.original_path:
            fields.append({"key": "Path", "value": f"`{alert.original_path}`"})

        return template.render(title=title,
                               alert=alert,
                               subtitle=subtitle,
                               report_link=report_link,
                               fields=fields,
                               target_type=integration_params.get("target_type"),
                               target_channel=integration_params.get("target_channel"),
                               recipients=integration_params.get("recipients"),
                               included_html=integration_params.get("included_html"),
                               )


    def _get_source_freshness_template(
        self, alert: SourceFreshnessAlertModel, template=None, integration_params=None, *args, **kwargs
    ):
        title = f"{self._get_display_name(alert.status)}: {alert.summary}"
        subtitle = self._get_alert_sub_title(alert)
        report_link = self._get_report_link_if_applicable(alert)

        fields = [
            {"key":"Tags", "value": self._get_tags_field_if_applicable(alert)},
            {"key":"Owners", "value": self._get_owners_field_if_applicable(alert)},
            {"key":"Subscribers", "value": self._get_subscribers_field_if_applicable(alert)}
        ]

        if alert.freshness_description:
            fields.append({"key": "Description", "value": f'_{alert.freshness_description or "No description"}_'})

        if alert.status == "runtime error":
            fields.append({"key": "Result message", "value": f"Failed to calculate the source freshness\n```{alert.error}```"})
        else:
            fields.append({"key": "Result message", "value": f"```{alert.result_description}```"})
            fields.append({"key": "Time elapsed", "value":f"{timedelta(seconds=alert.max_loaded_at_time_ago_in_s) if alert.max_loaded_at_time_ago_in_s else 'N/A'}"})
            fields.append({"key": "Last record at", "value":f"{alert.max_loaded_at}"})
            fields.append({"key": "Sampled at", "value":f"{alert.snapshotted_at_str}"})
        if alert.error_after:
            fields.append({"key": "Error after", "value": f"`{alert.error_after}`"})
            fields.append({"key": "Warn after", "value": f"`{alert.warn_after}`"})
            fields.append({"key": "Filter", "value": f"`{alert.filter}`"})
        if alert.path:
            fields.append({"key": "Path", "value": f"`{alert.path}`"})

        return template.render(title=title,
                               alert=alert,
                               subtitle=subtitle,
                               report_link=report_link,
                               fields=fields,
                               target_type=integration_params.get("target_type"),
                               target_channel=integration_params.get("target_channel"),
                               recipients=integration_params.get("recipients"),
                               included_html=integration_params.get("included_html"),
                               )

    def _get_group_by_table_template(
        self, alert: GroupedByTableAlerts, template=None, integration_params=None, *args, **kwargs
    ):
        alerts = alert.alerts
        title = f"{self._get_display_name(alert.status)}: {alert.summary}"
        subtitle = ""

        if alert.model_errors:
            subtitle = (
                subtitle
                + (" | " + f"&#x1F635; Model errors: {len(alert.model_errors)}")
                if subtitle
                else f"&#x1F635; Model errors: {len(alert.model_errors)}"
            )
        if alert.test_failures:
            subtitle = (
                subtitle
                + (" | " + f"&#x1F53A; Test failures: {len(alert.test_failures)}")
                if subtitle
                else f"&#x1F53A; Test failures: {len(alert.test_failures)}"
            )
        if alert.test_warnings:
            subtitle = (
                subtitle
                + (" | " + f"&#x26A0; Test warnings: {len(alert.test_warnings)}")
                if subtitle
                else f"&#x26A0; Test warnings: {len(alert.test_warnings)}"
            )
        if alert.test_errors:
            subtitle = (
                subtitle + (" | " + f"&#x2757; Test errors: {len(alert.test_errors)}")
                if subtitle
                else f"&#x2757; Test errors: {len(alert.test_errors)}"
            )

        report_link = self._get_report_link_if_applicable(alert)

        tags = list_of_lists_of_strings_to_comma_delimited_unique_strings(
            [alert.tags or [] for alert in alerts]
        )
        owners = list_of_lists_of_strings_to_comma_delimited_unique_strings(
            [alert.owners or [] for alert in alerts]
        )
        subscribers = list_of_lists_of_strings_to_comma_delimited_unique_strings(
            [alert.subscribers or [] for alert in alerts]
        )
        fields = [
            {"key":"Tags", "value": f'_{tags if tags else "No tags"}_'},
            {"key":"Owners", "value": f'_{owners if owners else "No owners"}_'},
            {"key":"Subscribers", "value": f'_{subscribers if subscribers else "No subscribers"}_'}
        ]
        return template.render(title=title,
                               alert=alert,
                               subtitle=subtitle,
                               report_link=report_link,
                               fields=fields,
                               model_errors=alert.model_errors,
                               test_failures=alert.test_failures,
                               test_warnings=alert.test_warnings,
                               test_errors=alert.test_errors,
                               target_type=integration_params.get("target_type"),
                               target_channel=integration_params.get("target_channel"),
                               recipients=integration_params.get("recipients"),
                               included_html=integration_params.get("included_html"),
                               )

    def _get_alerts_group_template(self, alert: AlertsGroup, template=None, integration_params=None, *args, **kwargs):  # type: ignore[override]
        title = f"{self._get_display_name(alert.status)}: {alert.summary}"

        subtitle = ""
        if alert.model_errors:
            subtitle = (
                subtitle
                + (" | " + f"⚫ Model errors: {len(alert.model_errors)}")
                if subtitle
                else f"⚫ Model errors: {len(alert.model_errors)}"
            )
        if alert.test_failures:
            subtitle = (
                subtitle
                + (" | " + f"🛑 Test failures: {len(alert.test_failures)}")
                if subtitle
                else f"🛑 Test failures: {len(alert.test_failures)}"
            )
        if alert.test_warnings:
            subtitle = (
                subtitle
                + (" | " + f"🔶 Test warnings: {len(alert.test_warnings)}")
                if subtitle
                else f"🔶 Test warnings: {len(alert.test_warnings)}"
            )
        if alert.test_errors:
            subtitle = (
                subtitle + (" | " + f"🔴 Test errors: {len(alert.test_errors)}")
                if subtitle
                else f"🔴 Test errors: {len(alert.test_errors)}"
            )
        return template.render(title=title,
                               alert=alert,
                               subtitle=subtitle,
                               model_errors=alert.model_errors,
                               test_failures=alert.test_failures,
                               test_warnings=alert.test_warnings,
                               test_errors=alert.test_errors,
                               target_type=integration_params.get("target_type"),
                               target_channel=integration_params.get("target_channel"),
                               recipients=integration_params.get("recipients"),
                               included_html=integration_params.get("included_html"),
                               )


    def _get_fallback_template(
        self,
        alert: Union[
            TestAlertModel,
            ModelAlertModel,
            SourceFreshnessAlertModel,
            GroupedByTableAlerts,
            AlertsGroup,
        ],
        template=None,
        integration_params=None,
        *args,
        **kwargs,
    ):
        return template.render(title="Oops, we failed to format the alert ! -_-' Please share this with the Elementary team via <https://elementary-data.com/community> or a <https://github.com/elementary-data/elementary/issues/new|GitHub> issue.",
                               text=f"```{json.dumps(alert.data, indent=2)}",
                               target_type=integration_params.get("target_type"),
                               target_channel=integration_params.get("target_channel"),
                               recipients=integration_params.get("recipients"),
                               alert=alert
                               )

    def _get_test_message_template(self, template=None, integration_params=None, *args, **kwargs):
        return template.render(title="This is a test message generated by Elementary!",
                               text=f"Elementary monitor ran successfully on {datetime.now().strftime('%Y-%m-%d %H:%M')}",
                               target_type=integration_params.get("target_type"),
                               target_channel=integration_params.get("target_channel"),
                               recipients=integration_params.get("recipients"),
                               )

    def _get_integrations_params(
            self,
            alert: Union[
                TestAlertModel,
                ModelAlertModel,
                SourceFreshnessAlertModel,
                GroupedByTableAlerts,
                BaseAlertsGroup,
                None
            ],
            *args,
            **kwargs,
    ) -> Dict[str, Any]:
        integration_params = dict()
        if alert is not None and alert.unified_meta.get("webhook_target_type"):
            target_type = alert.unified_meta.get("webhook_target_type")
            logger.debug(f"Using target_type from meta: {target_type}")
        else:
            target_type = self.config.webhook_target_type
        integration_params.update({"target_type": target_type})

        if alert is not None and alert.unified_meta.get("webhook_target_channel"):
            target_channel = alert.unified_meta.get("webhook_target_channel")
            logger.debug(f"Using target_channel from meta: {target_channel}")
        else:
            target_channel = self.config.webhook_target_channel
        integration_params.update({"target_channel": target_channel})

        integration_params.update({"recipients": self._get_recipients(alert)})

        return integration_params


    def send_alert(self, alert: Union[
        TestAlertModel,
        ModelAlertModel,
        SourceFreshnessAlertModel,
        GroupedByTableAlerts,
        BaseAlertsGroup,
    ], *args, **kwargs) -> bool:

        if self.config.webhook_template_dir is not None:
            env = Environment(loader=FileSystemLoader(self.config.webhook_template_dir))
        else:
            env = Environment(
                loader=PackageLoader("elementary.monitor.data_monitoring.alerts.integrations.webhook", "templates"))
        template = env.get_template(self.config.webhook_template)
        integration_params = self._get_integrations_params(alert)

        try:
            logger.debug("Sending alert via Webhook.")
            if self.config.webhook_included_html_template is not None:
                included_html_template = env.get_template(self.config.webhook_included_html_template)
                included_html = self._get_alert_template(alert, template=included_html_template, integration_params=integration_params)
                included_html = simplejson.encoder.JSONEncoderForHTML().encode(included_html)
                integration_params.update({"included_html": included_html})
            request_body= self._get_alert_template(alert, template=template, integration_params=integration_params)
            response = self.send(request_body)
            sent_successfully = response.ok is True
        except Exception as e:
            logger.error(
                f"Unable to send alert via Webhook: {e}\nSending fallback template."
            )
            sent_successfully = False

        if not sent_successfully:
            try:
                response = self.send(self._get_fallback_template(alert, template=template, integration_params=integration_params))
                fallback_sent_successfully = response.ok is True
            except Exception as e:
                logger.error(f"Unable to send alert fallback via Webhook: {e}")
                fallback_sent_successfully = False
            sent_successfully = fallback_sent_successfully

        return sent_successfully

    def send_test_message(self, *args, **kwargs) -> bool:

        if self.config.webhook_template_dir is not None:
            env = Environment(loader=FileSystemLoader(self.config.webhook_template_dir))
        else:
            env = Environment(
                loader=PackageLoader("elementary.monitor.data_monitoring.alerts.integrations.webhook", "templates"))
        template = env.get_template(self.config.webhook_template)
        integration_params = self._get_integrations_params(None)

        self.send(self._get_test_message_template(template=template, integration_params=integration_params))

    def send(self, request_body):
        proxies = None
        if self.config.webhook_http_proxy is not None:
            proxies = {
                "http": self.config.webhook_http_proxy,
                "https": self.config.webhook_http_proxy
            }
        auth = None
        headers = {"Content-Type": self.config.webhook_http_content_type} if self.config.webhook_http_content_type is not None else {}
        if self.config.webhook_http_auth_scheme == "basic":
            auth = HTTPBasicAuth(self.config.webhook_http_auth_basic_user, self.config.webhook_http_auth_basic_pass)
        elif self.config.webhook_http_auth_scheme == "oauth2":
            scope = None
            if self.config.webhook_http_auth_oauth2_scope is not None:
                scope = str(self.config.webhook_http_auth_oauth2_scope).split(",")
            client = BackendApplicationClient(client_id=self.config.webhook_http_auth_oauth2_client_id, scope=scope)
            oauth = OAuth2Session(client=client)
            secret = self.config.webhook_http_auth_oauth2_secret.__getitem__(0) if isinstance(self.config.webhook_http_auth_oauth2_secret, tuple) else self.config.webhook_http_auth_oauth2_secret
            try:
                auth_response = oauth.fetch_token(token_url=self.config.webhook_http_auth_oauth2_url, client_id=self.config.webhook_http_auth_oauth2_client_id,
                                      client_secret=secret, proxies=proxies)
                access_token = auth_response['access_token']

            except Exception as e:
                logger.error(
                    f"Unable to authenticate with oauth: {e}"
                )
                raise e

            headers.update({
                "Authorization": f"Bearer {access_token}"
            })
        if self.config.webhook_http_content_type == 'application/json':
            return requests.post(self.config.webhook_http_url, json=json.loads(request_body), proxies=proxies, auth=auth, headers=headers, verify=self.config.webhook_http_ssl_verify)
        else:
            return requests.post(self.config.webhook_http_url, data=request_body, proxies=proxies, auth=auth, headers=headers, verify=self.config.webhook_http_ssl_verify)

