import json
import pathlib
import tempfile
import unittest

from scripts.tests.release import load

manifest = load("manifest")


class GateFlags(unittest.TestCase):
    def test_maps_gate_labels_and_ignores_others(self):
        flags = manifest.gate_flags_from_labels(
            ["risk:breaking", "area:ops", "GATE:mig", "risk:realm", "docs"]
        )
        self.assertEqual(flags, ["breaking", "migration", "realm"])

    def test_only_pull_requests_merged_into_the_release_range_count(self):
        merged = "\n".join(
            [
                "aaa\trisk:breaking",  # in range
                "bbb\tgate:mig",  # merged after the release commit
                "ccc\trisk:realm",  # before the previous final tag
                "aaa\tdocs",
                "malformed line",
            ]
        )
        self.assertEqual(
            manifest.labels_in_range(merged, "aaa\nddd\n"), ["docs", "risk:breaking"]
        )


class BuildManifest(unittest.TestCase):
    def test_refuses_a_short_git_sha(self):
        with self.assertRaisesRegex(SystemExit, "full lowercase SHA"):
            manifest.build_manifest(
                component="ezu", version="0.1.0", git_sha="deadbeef", tag="v0.1.0",
                dist=pathlib.Path("."), images=[], attestations=[], gate_flags=[],
            )

    def test_refuses_a_latest_image_tag(self):
        with self.assertRaisesRegex(SystemExit, "latest"):
            manifest.parse_image("ezu:latest=sha256:" + "a" * 64)

    def test_lists_binaries_by_digest_and_images_separately(self):
        with tempfile.TemporaryDirectory() as tmp:
            dist = pathlib.Path(tmp)
            (dist / "ezu-0.1.0-x86_64-unknown-linux-gnu").write_bytes(b"bin")
            (dist / "ezu-0.1.0-image.tar").write_bytes(b"tar")
            for name in ("SHA256SUMS", "ezu-release.json", "ezu.cdx.json", "RELEASE_NOTES.md"):
                (dist / name).write_text("{}\n")
            image = manifest.parse_image("ezu:v0.1.0=sha256:" + "b" * 64)
            doc = manifest.build_manifest(
                component="ezu", version="0.1.0", git_sha="a" * 40, tag="v0.1.0",
                dist=dist, images=[image], attestations=[], gate_flags=["breaking"],
            )
        self.assertEqual(doc["schema"], manifest.SCHEMA)
        self.assertEqual([b["name"] for b in doc["binaries"]], ["ezu-0.1.0-x86_64-unknown-linux-gnu"])
        self.assertTrue(doc["binaries"][0]["digest"].startswith("sha256:"))
        self.assertEqual(doc["images"], [{"name": "ezu:v0.1.0", "digest": "sha256:" + "b" * 64}])

    def test_record_attestations_drops_empty_urls(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = pathlib.Path(tmp) / "m.json"
            path.write_text(json.dumps({"attestations": [], "component": "ezu"}))
            manifest.main(["record-attestations", str(path), "https://example.invalid/1", ""])
            doc = json.loads(path.read_text())
        self.assertEqual(doc["attestations"], ["https://example.invalid/1"])
        self.assertEqual(doc["component"], "ezu")


if __name__ == "__main__":
    unittest.main()
