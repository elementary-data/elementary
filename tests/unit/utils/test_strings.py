from elementary.utils.strings import prettify_and_dedup_list


def test_prettify_and_dedup_list():
    list_prettified = prettify_and_dedup_list(["name1", "name2", "name2"])
    assert list_prettified == "name1, name2"

    assert prettify_and_dedup_list("name1, name2, name2") == "name1, name2"

    string_of_list_prettified = prettify_and_dedup_list('["name1", "name2", "name2"]')
    assert string_of_list_prettified == "name1, name2"
