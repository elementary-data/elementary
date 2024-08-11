from typing import Dict, Generic, List, Optional, TypeVar, Union

T = TypeVar("T")
TreeT = Dict[str, Union[List[T], "TreeT"]]


class TreeBuilder(Generic[T]):
    def __init__(self, seperator: str, files_keywork: str = "__files__") -> None:
        self.seperator = seperator
        self.files_keywork = files_keywork
        self._tree: TreeT = {}

    def add(self, path: Optional[str], data: Optional[T]) -> None:
        if path is None or data is None:
            return
        parts = path.split(self.seperator)
        current: dict = self._tree
        for part in parts[:-1]:
            if isinstance(current, list):
                raise ValueError(
                    f"Path parts cannot contain files keyword: {self.files_keywork}"
                )
            current = current.setdefault(part, {})
        if self.files_keywork in current:
            if id not in current[self.files_keywork]:
                current[self.files_keywork].append(data)
        else:
            current[self.files_keywork] = [data]

    def get_tree(self) -> TreeT:
        return self._tree
