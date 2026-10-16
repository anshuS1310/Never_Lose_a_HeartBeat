"""Preallocated emergency space for one disk-full recovery attempt."""
import os
from pathlib import Path

SIZE = 16 * 1024 * 1024


def path_for(db_path):
    return Path(str(db_path) + ".reserve")


def ensure(db_path):
    path = path_for(db_path)
    if path.exists():
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    try:
        block = b"\0" * (1024 * 1024)
        for _ in range(SIZE // len(block)):
            os.write(descriptor, block)
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def release(db_path):
    path = path_for(db_path)
    try:
        path.unlink()
        return True
    except FileNotFoundError:
        return False
