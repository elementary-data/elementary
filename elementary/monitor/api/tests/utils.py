from typing import Any, Dict, Optional


def get_table_full_name(
    database_name: Optional[str],
    schema_name: Optional[str],
    table_name: Optional[str],
) -> str:
    if not table_name:
        return ""

    table_full_name_parts = [
        name
        for name in [
            database_name,
            schema_name,
            table_name,
        ]
        if name
    ]
    table_full_name = ".".join(table_full_name_parts).lower()
    return table_full_name


def get_display_name(name: str) -> str:
    return name.replace("_", " ").title()


def get_test_configuration(
    test_type: Optional[str], name: str, test_params: Dict
) -> Dict[str, Any]:
    if test_type is None:
        return dict()
    if test_type == "dbt_test":
        return dict(
            test_name=name,
            test_params=test_params,
        )
    else:
        time_bucket_configuration = test_params.get("time_bucket") or {}
        time_bucket_count = time_bucket_configuration.get("count") or 1
        time_bucket_period = time_bucket_configuration.get("period") or "day"
        return dict(
            test_name=name,
            timestamp_column=test_params.get("timestamp_column"),
            testing_timeframe=f"{time_bucket_count} {time_bucket_period}{'s' if time_bucket_count > 1 else ''}",
            anomaly_threshold=test_params.get("sensitivity")
            or test_params.get("anomaly_sensitivity"),
        )


def get_normalized_full_path(
    package_name: Optional[str], original_path: Optional[str]
) -> Optional[str]:
    if not original_path:
        return None
    if package_name:
        return f"{package_name}/{original_path}"
    return original_path
