import unittest
import uuid

from scripts.tests.release import load

sbom = load("sbom")

METADATA = {
    "workspace_members": ["ezu 0.1.0 (path+file:///workspace)"],
    "packages": [
        {"id": "ezu 0.1.0 (path+file:///workspace)", "name": "ezu", "version": "0.1.0", "license": "Apache-2.0"},
        {
            "id": "libc 0.2.0 (registry+https://github.com/rust-lang/crates.io-index)",
            "name": "libc", "version": "0.2.0", "license": "MIT/Apache-2.0",
        },
    ],
}


class BomFromMetadata(unittest.TestCase):
    def test_names_the_crate_with_the_fields_attest_sbom_requires(self):
        doc = sbom.bom_from_metadata(METADATA)
        self.assertEqual(doc["bomFormat"], "CycloneDX")
        uuid.UUID(doc["serialNumber"].removeprefix("urn:uuid:"))
        self.assertEqual(doc["metadata"]["component"]["purl"], "pkg:cargo/ezu@0.1.0")
        self.assertEqual(doc["metadata"]["component"]["type"], "application")

    def test_lists_dependencies_with_spdx_license_expressions(self):
        [dep] = sbom.bom_from_metadata(METADATA)["components"]
        self.assertEqual(dep["purl"], "pkg:cargo/libc@0.2.0")
        self.assertEqual(dep["licenses"], [{"expression": "MIT OR Apache-2.0"}])

    def test_serial_number_is_stable_for_the_same_lockfile(self):
        self.assertEqual(
            sbom.bom_from_metadata(METADATA)["serialNumber"], sbom.bom_from_metadata(METADATA)["serialNumber"]
        )

    def test_refuses_metadata_without_the_ezu_package(self):
        with self.assertRaisesRegex(SystemExit, "did not include package ezu"):
            sbom.bom_from_metadata({"workspace_members": [], "packages": []})


if __name__ == "__main__":
    unittest.main()
