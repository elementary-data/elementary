"""Tests for elementary monitor dbt_project configuration."""

import os

FILE_DIR = os.path.dirname(os.path.realpath(__file__))
DBT_PROJECT_PATH = os.path.normpath(
    os.path.join(
        FILE_DIR,
        "..",
        "..",
        "..",
        "elementary",
        "monitor",
        "dbt_project",
        "dbt_project.yml",
    )
)


def test_databricks_target_uses_delta_file_format():
    """When target.type is databricks, file_format should be delta."""
    with open(DBT_PROJECT_PATH) as f:
        content = f.read()
    assert "databricks" in content, "databricks must be in target.type condition for delta"
    assert "file_format" in content
    assert "delta" in content
    assert "spark" in content
    assert "fabricspark" in content
