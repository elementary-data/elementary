from typing import Optional

BUCKET_PATH_SEP = "/"


def dirname(bucket_path: str) -> Optional[str]:
    dir_path = BUCKET_PATH_SEP.join(bucket_path.split(BUCKET_PATH_SEP)[:-1])
    return dir_path or None


def basename(bucket_path: str) -> str:
    return bucket_path.rsplit(BUCKET_PATH_SEP, 1)[-1]


def join_path(path_parts: list) -> str:
    return BUCKET_PATH_SEP.join(path_parts)
