import pytest

from elementary.monitor.data_monitoring.schema import FiltersSchema, FilterType


def test_empty_from_cli_params():
    cli_filter = ()
    cli_excludes = ()
    filter_schema = FiltersSchema.from_cli_params(cli_filter, cli_excludes)
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
    cli_excludes = ()
    filter_schema = FiltersSchema.from_cli_params(cli_filter, cli_excludes)
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
    cli_excludes = ()
    filter_schema = FiltersSchema.from_cli_params(cli_filter, cli_excludes)
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
    cli_excludes = ()
    filter_schema = FiltersSchema.from_cli_params(cli_filter, cli_excludes)
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
    cli_excludes = ()
    filter_schema = FiltersSchema.from_cli_params(cli_filter, cli_excludes)
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
    cli_excludes = ()
    filter_schema = FiltersSchema.from_cli_params(cli_filter, cli_excludes)
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
    cli_excludes = ()
    filter_schema = FiltersSchema.from_cli_params(cli_filter, cli_excludes)
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
    cli_excludes = ()
    filter_schema = FiltersSchema.from_cli_params(cli_filter, cli_excludes)
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
    cli_excludes = ()
    filter_schema = FiltersSchema.from_cli_params(cli_filter, cli_excludes)
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
    cli_excludes = ()
    filter_schema = FiltersSchema.from_cli_params(cli_filter, cli_excludes)
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
        cli_excludes = ()
        FiltersSchema.from_cli_params(cli_filter, cli_excludes)

    with pytest.raises(ValueError):
        cli_filter = ("statuses:warn,freddy",)
        cli_excludes = ()
        FiltersSchema.from_cli_params(cli_filter, cli_excludes)

    with pytest.raises(ValueError):
        cli_filter = ("statuses:warn", "statuses:freddy")
        cli_excludes = ()
        FiltersSchema.from_cli_params(cli_filter, cli_excludes)

    cli_filter = ("statuses:warn",)
    cli_excludes = ()
    filter_schema = FiltersSchema.from_cli_params(cli_filter, cli_excludes)
    assert len(filter_schema.tags) == 0
    assert len(filter_schema.models) == 0
    assert len(filter_schema.owners) == 0
    assert len(filter_schema.statuses) == 1
    assert filter_schema.statuses[0].values == ["warn"]
    assert len(filter_schema.resource_types) == 0

    cli_filter = ("statuses:warn,fail",)
    cli_excludes = ()
    filter_schema = FiltersSchema.from_cli_params(cli_filter, cli_excludes)
    assert len(filter_schema.tags) == 0
    assert len(filter_schema.models) == 0
    assert len(filter_schema.owners) == 0
    assert len(filter_schema.statuses) == 1
    assert sorted(filter_schema.statuses[0].values) == sorted(["fail", "warn"])
    assert len(filter_schema.resource_types) == 0

    cli_filter = ("statuses:warn", "statuses:fail")
    cli_excludes = ()
    filter_schema = FiltersSchema.from_cli_params(cli_filter, cli_excludes)
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
        cli_excludes = ()
        FiltersSchema.from_cli_params(cli_filter, cli_excludes)

    with pytest.raises(ValueError):
        cli_filter = ("resource_types:test,freddy",)
        cli_excludes = ()
        FiltersSchema.from_cli_params(cli_filter, cli_excludes)

    with pytest.raises(ValueError):
        cli_filter = ("resource_types:test", "resource_types:freddy")
        cli_excludes = ()
        FiltersSchema.from_cli_params(cli_filter, cli_excludes)

    cli_filter = ("resource_types:test",)
    cli_excludes = ()
    filter_schema = FiltersSchema.from_cli_params(cli_filter, cli_excludes)
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
    cli_excludes = ()
    filter_schema = FiltersSchema.from_cli_params(cli_filter, cli_excludes)
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
    cli_excludes = ()
    filter_schema = FiltersSchema.from_cli_params(cli_filter, cli_excludes)
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
    cli_excludes = ()
    filter_schema = FiltersSchema.from_cli_params(cli_filter, cli_excludes)
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
    cli_excludes = ()
    filter_schema = FiltersSchema.from_cli_params(cli_filter, cli_excludes)
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


def test_exclude_filters():
    cli_filter = ("tags:tag1",)
    cli_excludes = ("tags:tag2",)
    filter_schema = FiltersSchema.from_cli_params(cli_filter, cli_excludes)
    assert len(filter_schema.tags) == 2
    assert filter_schema.tags[0].values == ["tag1"]
    assert filter_schema.tags[0].type == FilterType.IS
    assert filter_schema.tags[1].values == ["tag2"]
    assert filter_schema.tags[1].type == FilterType.IS_NOT
    assert len(filter_schema.owners) == 0
    assert len(filter_schema.models) == 0
    assert len(filter_schema.statuses) == 1
    assert sorted(filter_schema.statuses[0].values) == sorted(
        ["fail", "error", "runtime error", "warn"]
    )
    assert len(filter_schema.resource_types) == 0


def test_exclude_statuses_filters():
    cli_filters = ()
    cli_excludes = ("statuses:fail",)
    filter_schema = FiltersSchema.from_cli_params(cli_filters, cli_excludes)
    assert len(filter_schema.tags) == 0
    assert len(filter_schema.models) == 0
    assert len(filter_schema.owners) == 0
    assert len(filter_schema.statuses) == 2
    assert filter_schema.statuses[0].values == ["fail"]
    assert filter_schema.statuses[0].type == FilterType.IS_NOT
    assert sorted(filter_schema.statuses[1].values) == sorted(
        ["fail", "error", "runtime error", "warn"]
    )
    assert filter_schema.statuses[1].type == FilterType.IS
