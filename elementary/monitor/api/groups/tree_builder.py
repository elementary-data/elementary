from typing import Dict, Generic, List, Optional, TypeVar, Union

T = TypeVar("T")
TreeT = Dict[str, Union[List[T], "TreeT"]]


class TreeBuilder(Generic[T]):
    def __init__(self, separator: str, files_keyword: str = "__files__") -> None:
        self.separator = separator
        self.files_keyword = files_keyword
        self._tree: TreeT = {}

    def add(self, path: Optional[str], data: Optional[T]) -> None:
        if path is None or data is None:
            return
        parts = path.split(self.separator)
        current: dict = self._tree
        for part in parts[:-1]:
            if isinstance(current, list):
                raise ValueError(
                    f"Path parts cannot contain files keyword: {self.files_keyword}"
                )
            current = current.setdefault(part, {})
        if self.files_keyword in current:
            if id not in current[self.files_keyword]:
                current[self.files_keyword].append(data)
        else:
            current[self.files_keyword] = [data]

    def get_tree(self) -> TreeT:
        return self._tree
