#!/usr/bin/env python3
"""Require the complete upstream core asset inventory before publishing a build."""

import argparse
import hashlib
from pathlib import Path


def verify(directory: Path, version: str) -> int:
    inventory = Path(__file__).with_name("fork-artifacts.txt")
    expected = {
        name.format(version=version)
        for name in inventory.read_text().splitlines()
        if name.strip()
    }
    files = {path.name: path for path in directory.iterdir() if path.is_file()}
    # Permit repeat verification of the same downloaded artifacts.
    files.pop("SHA256SUMS", None)
    missing = expected - files.keys()
    unexpected = files.keys() - expected
    empty = {name for name, path in files.items() if path.stat().st_size == 0}
    if missing or unexpected or empty:
        for label, names in [("Missing", missing), ("Unexpected", unexpected), ("Empty", empty)]:
            for name in sorted(names):
                print(f"{label} artifact: {name}")
        return 1
    lines = []
    for name in sorted(expected):
        with files[name].open("rb") as source:
            digest = hashlib.file_digest(source, "sha256").hexdigest()
        lines.append(f"{digest}  {name}\n")
    (directory / "SHA256SUMS").write_text("".join(lines))
    print(f"Verified all {len(expected)} upstream core artifacts; generated SHA256SUMS")
    return 0


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("directory", type=Path)
    parser.add_argument("version")
    args = parser.parse_args()
    raise SystemExit(verify(args.directory, args.version))
