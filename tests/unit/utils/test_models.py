from elementary.utils.models import get_shortened_model_name


def test_shorten_none():
    shortened = get_shortened_model_name(None)
    assert shortened is None


def test_shorten_1_part():
    shortened = get_shortened_model_name("foo")
    assert shortened == "foo"


def test_shorten_3_parts():
    shortened = get_shortened_model_name("models.foo.bar")
    assert shortened == "bar"


def test_shorten_4_parts():
    shortened = get_shortened_model_name("models.foo.bar.v2")
    assert shortened == "bar.v2"
