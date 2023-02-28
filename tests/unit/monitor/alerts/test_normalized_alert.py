import json

from elementary.monitor.fetchers.alerts.normalized_alert import (
    ALERT_FIELDS_KEY,
    ALERT_SUPRESSION_INTERVAL_KEY,
    ALERTS_CONFIG_KEY,
    CHANNEL_KEY,
    COLUMN_FIELD,
    DEFAULT_ALERT_FIELDS,
    MODEL_META_KEY,
    OWNERS_FIELD,
    SUBSCRIBERS_KEY,
    TABLE_FIELD,
    TEST_META_KEY,
    NormalizedAlert,
)


def test_flatten_meta():
    alert = dict(
        test_meta=json.dumps(
            {
                SUBSCRIBERS_KEY: ["freddie", "dredd"],
                CHANNEL_KEY: "my_channel",
                ALERTS_CONFIG_KEY: {
                    ALERT_SUPRESSION_INTERVAL_KEY: 1,
                    CHANNEL_KEY: "my_other_channel",
                },
            }
        ),
        model_meta=json.dumps(
            dict(
                tags=["a tag"],
            )
        ),
    )
    normalized_alert = NormalizedAlert(alert)
    flatten_test_meta = normalized_alert._flatten_meta(TEST_META_KEY)
    flatten_model_meta = normalized_alert._flatten_meta(MODEL_META_KEY)
    assert json.dumps(flatten_test_meta, sort_keys=True) == json.dumps(
        {
            SUBSCRIBERS_KEY: ["freddie", "dredd"],
            CHANNEL_KEY: "my_other_channel",
            ALERT_SUPRESSION_INTERVAL_KEY: 1,
        },
        sort_keys=True,
    )
    assert json.dumps(flatten_model_meta, sort_keys=True) == json.dumps(
        dict(tags=["a tag"]), sort_keys=True
    )


def test_get_alert_meta_attrs():
    # Subscribers both for the test and the model
    alert = {
        TEST_META_KEY: json.dumps(
            {
                ALERTS_CONFIG_KEY: {
                    SUBSCRIBERS_KEY: ["freddie", "dredd"],
                }
            }
        ),
        MODEL_META_KEY: json.dumps(
            {
                ALERTS_CONFIG_KEY: {
                    SUBSCRIBERS_KEY: "nick",
                }
            }
        ),
    }
    normalized_alert = NormalizedAlert(alert)
    assert normalized_alert._get_alert_meta_attrs(SUBSCRIBERS_KEY) == [
        "freddie",
        "dredd",
        "nick",
    ]

    # Only test subscribers
    alert = {
        TEST_META_KEY: json.dumps(
            {
                ALERTS_CONFIG_KEY: {
                    SUBSCRIBERS_KEY: ["freddie", "dredd"],
                }
            }
        )
    }
    normalized_alert = NormalizedAlert(alert)
    assert normalized_alert._get_alert_meta_attrs(SUBSCRIBERS_KEY) == [
        "freddie",
        "dredd",
    ]

    # Only model subscribers
    alert = {
        MODEL_META_KEY: json.dumps(
            {
                ALERTS_CONFIG_KEY: {
                    SUBSCRIBERS_KEY: "nick",
                }
            }
        ),
    }
    normalized_alert = NormalizedAlert(alert)
    assert normalized_alert._get_alert_meta_attrs(SUBSCRIBERS_KEY) == ["nick"]

    # No subscribers
    alert = dict(
        test_meta=json.dumps(dict()),
        model_meta=json.dumps(dict()),
    )
    normalized_alert = NormalizedAlert(alert)
    assert normalized_alert._get_alert_meta_attrs(SUBSCRIBERS_KEY) == []


def test_get_alert_chennel():
    # Channel both for the test and the model
    alert = {
        TEST_META_KEY: json.dumps(
            {
                ALERTS_CONFIG_KEY: {
                    CHANNEL_KEY: "my_channel",
                }
            }
        ),
        MODEL_META_KEY: json.dumps(
            {
                ALERTS_CONFIG_KEY: {
                    CHANNEL_KEY: "my_other_channel",
                }
            }
        ),
    }
    normalized_alert = NormalizedAlert(alert)
    assert normalized_alert._get_alert_channel() == "my_channel"

    # Only test channel
    alert = {
        TEST_META_KEY: json.dumps(
            {
                ALERTS_CONFIG_KEY: {
                    CHANNEL_KEY: "my_channel",
                }
            }
        ),
    }
    normalized_alert = NormalizedAlert(alert)
    assert normalized_alert._get_alert_channel() == "my_channel"

    # Only model channel
    alert = {
        MODEL_META_KEY: json.dumps(
            {
                ALERTS_CONFIG_KEY: {
                    CHANNEL_KEY: "my_other_channel",
                }
            }
        ),
    }
    normalized_alert = NormalizedAlert(alert)
    assert normalized_alert._get_alert_channel() == "my_other_channel"

    # No channel
    alert = dict(
        test_meta=json.dumps(dict()),
        model_meta=json.dumps(dict()),
    )
    normalized_alert = NormalizedAlert(alert)
    assert normalized_alert._get_alert_channel() is None


def test_get_alert_suppression_interval():
    # Interval both for the test and the model
    alert = {
        TEST_META_KEY: json.dumps(
            {
                ALERTS_CONFIG_KEY: {
                    ALERT_SUPRESSION_INTERVAL_KEY: 1,
                }
            }
        ),
        MODEL_META_KEY: json.dumps(
            {
                ALERTS_CONFIG_KEY: {
                    ALERT_SUPRESSION_INTERVAL_KEY: 2,
                }
            }
        ),
    }
    normalized_alert = NormalizedAlert(alert)
    assert normalized_alert._get_alert_suppression_interval() == 1

    # Only test interval
    alert = {
        TEST_META_KEY: json.dumps(
            {
                ALERTS_CONFIG_KEY: {
                    ALERT_SUPRESSION_INTERVAL_KEY: 1,
                }
            }
        ),
    }
    normalized_alert = NormalizedAlert(alert)
    assert normalized_alert._get_alert_suppression_interval() == 1

    # Only model interval
    alert = {
        MODEL_META_KEY: json.dumps(
            {
                ALERTS_CONFIG_KEY: {
                    ALERT_SUPRESSION_INTERVAL_KEY: 2,
                }
            }
        ),
    }
    normalized_alert = NormalizedAlert(alert)
    assert normalized_alert._get_alert_suppression_interval() == 2

    # No interval
    alert = dict(
        test_meta=json.dumps(dict()),
        model_meta=json.dumps(dict()),
    )
    normalized_alert = NormalizedAlert(alert)
    assert normalized_alert._get_alert_suppression_interval() == 0


def test_get_alert_fields():
    # Alert fields both for the test and the model
    alert = {
        TEST_META_KEY: json.dumps(
            {
                ALERTS_CONFIG_KEY: {
                    ALERT_FIELDS_KEY: [TABLE_FIELD],
                }
            }
        ),
        MODEL_META_KEY: json.dumps(
            {
                ALERTS_CONFIG_KEY: {
                    ALERT_FIELDS_KEY: [COLUMN_FIELD, OWNERS_FIELD],
                }
            }
        ),
    }
    normalized_alert = NormalizedAlert(alert)
    assert normalized_alert._get_alert_fields() == [TABLE_FIELD]

    # Only test alert fields
    alert = {
        TEST_META_KEY: json.dumps(
            {
                ALERTS_CONFIG_KEY: {
                    ALERT_FIELDS_KEY: [TABLE_FIELD],
                }
            }
        )
    }
    normalized_alert = NormalizedAlert(alert)
    assert normalized_alert._get_alert_fields() == [TABLE_FIELD]

    # Only model alert fields
    alert = {
        MODEL_META_KEY: json.dumps(
            {
                ALERTS_CONFIG_KEY: {
                    ALERT_FIELDS_KEY: [COLUMN_FIELD, OWNERS_FIELD],
                }
            }
        ),
    }
    normalized_alert = NormalizedAlert(alert)
    assert normalized_alert._get_alert_fields() == [COLUMN_FIELD, OWNERS_FIELD]

    # No alert fields
    alert = dict(
        test_meta=json.dumps(dict()),
        model_meta=json.dumps(dict()),
    )
    normalized_alert = NormalizedAlert(alert)
    assert normalized_alert._get_alert_fields() == DEFAULT_ALERT_FIELDS
