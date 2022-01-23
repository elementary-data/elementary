from typing import Optional

from utils.anonymous_tracking import AnonymousTracking


def track_lineage_start(config_dir: str, profiles_dir: str, profile_data: dict, cli_properties: dict) -> \
        Optional['AnonymousTracking']:
    try:
        anonymous_tracking = AnonymousTracking(config_dir, profiles_dir, profile_data.get('anonymous_usage_tracking'))
        anonymous_tracking.init()
        cli_start_properties = {'platform_type': profile_data.get('type')}
        cli_start_properties.update(cli_properties)
        anonymous_tracking.send_event('cli-start', properties=cli_start_properties)
        return anonymous_tracking
    except Exception:
        pass
    return None


def track_lineage_end(anonymous_tracking: AnonymousTracking, lineage_properties: dict, query_history_properties: dict) \
        -> None:
    try:
        if anonymous_tracking is None:
            return

        cli_end_properties = dict()
        cli_end_properties.update(lineage_properties)
        cli_end_properties.update(query_history_properties)
        anonymous_tracking.send_event('cli-end', properties=cli_end_properties)
    except Exception:
        pass


def track_lineage_exception(anonymous_tracking: AnonymousTracking, exc: Exception) \
        -> None:
    try:
        if anonymous_tracking is None:
            return

        cli_exception_properties = dict()
        cli_exception_properties['exception_type'] = str(type(exc))
        cli_exception_properties['exception_content'] = str(exc)
        anonymous_tracking.send_event('cli-exception', properties=cli_exception_properties)
    except Exception:
        pass
