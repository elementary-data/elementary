from ruamel.yaml import YAML


def get_ordered_yaml() -> 'YAML':
    yaml = YAML()
    yaml.indent(mapping=2, sequence=4, offset=2)
    yaml.preserve_quotes = True
    return yaml
