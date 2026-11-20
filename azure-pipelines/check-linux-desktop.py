import base64
import hashlib
import json
from pathlib import Path
import sys
import zlib

import yaml


def verify(assets, update_config, version):
    image = assets / "T3-Code-Nightly-x86_64.AppImage"
    manifest = yaml.safe_load((assets / "nightly-linux.yml").read_text())
    entry = next(item for item in manifest["files"] if item["url"] == image.name)
    assert manifest["version"] == version, "Wrong updater manifest version"
    assert manifest["path"] == image.name, "Wrong primary updater asset"
    assert entry["size"] == image.stat().st_size, "Wrong AppImage size in manifest"
    with image.open("rb") as stream:
        digest = base64.b64encode(hashlib.file_digest(stream, "sha512").digest()).decode()
        assert entry["sha512"] == manifest["sha512"] == digest, "Wrong AppImage SHA-512"
        stream.seek(-4, 2)
        blockmap_size = int.from_bytes(stream.read(4), "big")
        assert 0 < blockmap_size == entry["blockMapSize"], "Wrong embedded blockmap size"
        stream.seek(-4 - blockmap_size, 2)
        blockmap = json.loads(zlib.decompress(stream.read(blockmap_size), wbits=-15))
        assert blockmap["files"], "Empty embedded blockmap"

    config = yaml.safe_load(update_config.read_text())
    assert {
        key: config[key] for key in ("provider", "owner", "repo", "channel", "releaseType")
    } == {
        "provider": "github",
        "owner": "MateuszKordasiewicz",
        "repo": "t3code",
        "channel": "nightly",
        "releaseType": "prerelease",
    }, "Wrong packaged update feed"
    print(f"Verified {image.name}, nightly-linux.yml, embedded blockmap and GitHub updater config")


if __name__ == "__main__":
    verify(Path(sys.argv[1]), Path(sys.argv[2]), sys.argv[3])
