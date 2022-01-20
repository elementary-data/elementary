from ruamel.yaml import YAML


class OrderedYaml(object):

    def __init__(self) -> None:
        self.ordered_yaml = YAML()
        self.ordered_yaml.indent(mapping=2, sequence=4, offset=2)
        self.ordered_yaml.preserve_quotes = True

    def load(self, file_path: str) -> dict:
        with open(file_path, 'r') as file_obj:
            return self.ordered_yaml.load(file_obj)

    def dump(self, data: dict, file_path: str) -> dict:
        with open(file_path, 'w') as file_obj:
            return self.ordered_yaml.dump(data, file_obj)

