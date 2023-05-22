from ruamel.yaml import YAML


class OrderedYaml:
    def __init__(self) -> None:
        self.ordered_yaml = YAML()
        self.ordered_yaml.indent(mapping=2, sequence=4, offset=2)
        self.ordered_yaml.preserve_quotes = True  # type: ignore[assignment]

    def load(self, file_path: str) -> dict:
        with open(file_path, "r", encoding="utf-8") as file_obj:
            return self.ordered_yaml.load(file_obj)

    def dump(self, data: dict, file_path: str) -> dict:
        with open(file_path, "w", encoding="utf-8") as file_obj:
            return self.ordered_yaml.dump(data, file_obj)

    def loads(self, yaml_str: str) -> dict:
        return self.ordered_yaml.load(yaml_str)
