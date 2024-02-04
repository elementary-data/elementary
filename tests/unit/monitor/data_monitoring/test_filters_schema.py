import pytest

from elementary.monitor.data_monitoring.schema import FiltersSchema


def test_empty_from_cli_params():
    cli_filter = ()
    filter_schema = FiltersSchema.from_cli_params(cli_filter)
    assert len(filter_schema.tags) == 0
    assert len(filter_schema.owners) == 0
    assert len(filter_schema.models) == 0
    assert len(filter_schema.statuses) == 1
    assert sorted(filter_schema.statuses[0].values) == sorted(
        ["fail", "error", "runtime error", "warn"]
    )
    assert len(filter_schema.resource_types) == 0


def test_tags_key_from_cli_params():
    cli_filter = ("tags:tag1",)
    filter_schema = FiltersSchema.from_cli_params(cli_filter)
    assert len(filter_schema.tags) == 1
    assert filter_schema.tags[0].values == ["tag1"]
    assert len(filter_schema.owners) == 0
    assert len(filter_schema.models) == 0
    assert len(filter_schema.statuses) == 1
    assert sorted(filter_schema.statuses[0].values) == sorted(
        ["fail", "error", "runtime error", "warn"]
    )
    assert len(filter_schema.resource_types) == 0

    cli_filter = ("tags:tag1,tag2",)
    filter_schema = FiltersSchema.from_cli_params(cli_filter)
    assert len(filter_schema.tags) == 1
    assert sorted(filter_schema.tags[0].values) == sorted(["tag1", "tag2"])
    assert len(filter_schema.owners) == 0
    assert len(filter_schema.models) == 0
    assert len(filter_schema.statuses) == 1
    assert sorted(filter_schema.statuses[0].values) == sorted(
        ["fail", "error", "runtime error", "warn"]
    )
    assert len(filter_schema.resource_types) == 0

    cli_filter = ("tags:tag1", "tags:tag2")
    filter_schema = FiltersSchema.from_cli_params(cli_filter)
    assert len(filter_schema.tags) == 2
    assert filter_schema.tags[0].values == ["tag1"]
    assert filter_schema.tags[1].values == ["tag2"]
    assert len(filter_schema.owners) == 0
    assert len(filter_schema.models) == 0
    assert len(filter_schema.statuses) == 1
    assert sorted(filter_schema.statuses[0].values) == sorted(
        ["fail", "error", "runtime error", "warn"]
    )
    assert len(filter_schema.resource_types) == 0


def test_owners_key_from_cli_params():
    cli_filter = ("owners:freddy",)
    filter_schema = FiltersSchema.from_cli_params(cli_filter)
    assert len(filter_schema.tags) == 0
    assert len(filter_schema.owners) == 1
    assert filter_schema.owners[0].values == ["freddy"]
    assert len(filter_schema.models) == 0
    assert len(filter_schema.statuses) == 1
    assert sorted(filter_schema.statuses[0].values) == sorted(
        ["fail", "error", "runtime error", "warn"]
    )
    assert len(filter_schema.resource_types) == 0

    cli_filter = ("owners:freddy,dredd",)
    filter_schema = FiltersSchema.from_cli_params(cli_filter)
    assert len(filter_schema.tags) == 0
    assert len(filter_schema.owners) == 1
    assert sorted(filter_schema.owners[0].values) == sorted(["freddy", "dredd"])
    assert len(filter_schema.models) == 0
    assert len(filter_schema.statuses) == 1
    assert sorted(filter_schema.statuses[0].values) == sorted(
        ["fail", "error", "runtime error", "warn"]
    )
    assert len(filter_schema.resource_types) == 0

    cli_filter = ("owners:freddy", "owners:dredd")
    filter_schema = FiltersSchema.from_cli_params(cli_filter)
    assert len(filter_schema.tags) == 0
    assert len(filter_schema.owners) == 2
    assert filter_schema.owners[0].values == ["freddy"]
    assert filter_schema.owners[1].values == ["dredd"]
    assert len(filter_schema.models) == 0
    assert len(filter_schema.statuses) == 1
    assert sorted(filter_schema.statuses[0].values) == sorted(
        ["fail", "error", "runtime error", "warn"]
    )
    assert len(filter_schema.resource_types) == 0


def test_models_key_from_cli_params():
    cli_filter = ("models:freddy",)
    filter_schema = FiltersSchema.from_cli_params(cli_filter)
    assert len(filter_schema.tags) == 0
    assert len(filter_schema.models) == 1
    assert filter_schema.models[0].values == ["freddy"]
    assert len(filter_schema.owners) == 0
    assert len(filter_schema.statuses) == 1
    assert sorted(filter_schema.statuses[0].values) == sorted(
        ["fail", "error", "runtime error", "warn"]
    )
    assert len(filter_schema.resource_types) == 0

    cli_filter = ("models:freddy,dredd",)
    filter_schema = FiltersSchema.from_cli_params(cli_filter)
    assert len(filter_schema.tags) == 0
    assert len(filter_schema.models) == 1
    assert sorted(filter_schema.models[0].values) == sorted(["freddy", "dredd"])
    assert len(filter_schema.owners) == 0
    assert len(filter_schema.statuses) == 1
    assert sorted(filter_schema.statuses[0].values) == sorted(
        ["fail", "error", "runtime error", "warn"]
    )
    assert len(filter_schema.resource_types) == 0

    cli_filter = ("models:freddy", "models:dredd")
    filter_schema = FiltersSchema.from_cli_params(cli_filter)
    assert len(filter_schema.tags) == 0
    assert len(filter_schema.models) == 2
    assert filter_schema.models[0].values == ["freddy"]
    assert filter_schema.models[1].values == ["dredd"]
    assert len(filter_schema.owners) == 0
    assert len(filter_schema.statuses) == 1
    assert sorted(filter_schema.statuses[0].values) == sorted(
        ["fail", "error", "runtime error", "warn"]
    )
    assert len(filter_schema.resource_types) == 0


def test_statuses_key_from_cli_params():
    with pytest.raises(ValueError):
        cli_filter = ("statuses:freddy",)
        FiltersSchema.from_cli_params(cli_filter)

    with pytest.raises(ValueError):
        cli_filter = ("statuses:warn,freddy",)
        FiltersSchema.from_cli_params(cli_filter)

    with pytest.raises(ValueError):
        cli_filter = ("statuses:warn", "statuses:freddy")
        FiltersSchema.from_cli_params(cli_filter)

    cli_filter = ("statuses:warn",)
    filter_schema = FiltersSchema.from_cli_params(cli_filter)
    assert len(filter_schema.tags) == 0
    assert len(filter_schema.models) == 0
    assert len(filter_schema.owners) == 0
    assert len(filter_schema.statuses) == 1
    assert filter_schema.statuses[0].values == ["warn"]
    assert len(filter_schema.resource_types) == 0

    cli_filter = ("statuses:warn,fail",)
    filter_schema = FiltersSchema.from_cli_params(cli_filter)
    assert len(filter_schema.tags) == 0
    assert len(filter_schema.models) == 0
    assert len(filter_schema.owners) == 0
    assert len(filter_schema.statuses) == 1
    assert sorted(filter_schema.statuses[0].values) == sorted(["fail", "warn"])
    assert len(filter_schema.resource_types) == 0

    cli_filter = ("statuses:warn", "statuses:fail")
    filter_schema = FiltersSchema.from_cli_params(cli_filter)
    assert len(filter_schema.tags) == 0
    assert len(filter_schema.models) == 0
    assert len(filter_schema.owners) == 0
    assert len(filter_schema.statuses) == 2
    assert filter_schema.statuses[0].values == ["warn"]
    assert filter_schema.statuses[1].values == ["fail"]
    assert len(filter_schema.resource_types) == 0


def test_resource_types_key_from_cli_params():
    with pytest.raises(ValueError):
        cli_filter = ("resource_types:freddy",)
        FiltersSchema.from_cli_params(cli_filter)

    with pytest.raises(ValueError):
        cli_filter = ("resource_types:test,freddy",)
        FiltersSchema.from_cli_params(cli_filter)

    with pytest.raises(ValueError):
        cli_filter = ("resource_types:test", "resource_types:freddy")
        FiltersSchema.from_cli_params(cli_filter)

    cli_filter = ("resource_types:test",)
    filter_schema = FiltersSchema.from_cli_params(cli_filter)
    assert len(filter_schema.tags) == 0
    assert len(filter_schema.models) == 0
    assert len(filter_schema.owners) == 0
    assert len(filter_schema.statuses) == 1
    assert sorted(filter_schema.statuses[0].values) == sorted(
        ["fail", "error", "runtime error", "warn"]
    )
    assert len(filter_schema.resource_types) == 1
    assert filter_schema.resource_types[0].values == ["test"]

    cli_filter = ("resource_types:test,model",)
    filter_schema = FiltersSchema.from_cli_params(cli_filter)
    assert len(filter_schema.tags) == 0
    assert len(filter_schema.models) == 0
    assert len(filter_schema.owners) == 0
    assert len(filter_schema.statuses) == 1
    assert sorted(filter_schema.statuses[0].values) == sorted(
        ["fail", "error", "runtime error", "warn"]
    )
    assert len(filter_schema.resource_types) == 1
    assert sorted(filter_schema.resource_types[0].values) == sorted(["test", "model"])

    cli_filter = ("resource_types:test", "resource_types:model")
    filter_schema = FiltersSchema.from_cli_params(cli_filter)
    assert len(filter_schema.tags) == 0
    assert len(filter_schema.models) == 0
    assert len(filter_schema.owners) == 0
    assert len(filter_schema.statuses) == 1
    assert sorted(filter_schema.statuses[0].values) == sorted(
        ["fail", "error", "runtime error", "warn"]
    )
    assert len(filter_schema.resource_types) == 2
    assert filter_schema.resource_types[0].values == ["test"]
    assert filter_schema.resource_types[1].values == ["model"]


def test_unsupported_key_from_cli_params():
    cli_filter = ("fake",)
    filter_schema = FiltersSchema.from_cli_params(cli_filter)
    assert len(filter_schema.tags) == 0
    assert len(filter_schema.owners) == 0
    assert len(filter_schema.models) == 0
    assert len(filter_schema.statuses) == 1
    assert sorted(filter_schema.statuses[0].values) == sorted(
        ["fail", "error", "runtime error", "warn"]
    )
    assert len(filter_schema.resource_types) == 0


def test_multiple_keys_from_cli_params():
    cli_filter = ("tags:tag1", "owners:freddy,dredd")
    filter_schema = FiltersSchema.from_cli_params(cli_filter)
    assert len(filter_schema.tags) == 1
    assert filter_schema.tags[0].values == ["tag1"]
    assert len(filter_schema.owners) == 1
    assert sorted(filter_schema.owners[0].values) == sorted(["freddy", "dredd"])
    assert len(filter_schema.models) == 0
    assert len(filter_schema.statuses) == 1
    assert sorted(filter_schema.statuses[0].values) == sorted(
        ["fail", "error", "runtime error", "warn"]
    )
    assert len(filter_schema.resource_types) == 0
