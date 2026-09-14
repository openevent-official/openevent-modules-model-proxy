import importlib.metadata
from pathlib import Path
import unittest
from unittest.mock import patch


class E2EEntryTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        path = Path(__file__).resolve().parents[1] / "test-e2e.sh"
        source = path.read_text().split("<<'PY'\n", 1)[1].split("\nPY\n", 1)[0]
        cls.preflight = compile(source, str(path), "exec")

    def test_accepts_installed_sdk_versions_satisfying_requirement(self):
        for version in ("0.8", "0.8.0", "0.8.0+local", "0.8.0.post1", "0.9.0rc1", "0.9.0.dev1", "1.0.0"):
            with self.subTest(version=version):
                with patch("importlib.metadata.version", return_value=version):
                    exec(self.preflight, {})

    def test_rejects_installed_sdk_versions_below_requirement(self):
        for version in ("0.7.9", "0.7.9+local", "0.8.0rc1", "0.8.0.dev1"):
            with self.subTest(version=version):
                with patch("importlib.metadata.version", return_value=version):
                    with self.assertRaisesRegex(SystemExit, "openevent-sdk>=0.8.0 must already be installed"):
                        exec(self.preflight, {})

    def test_missing_sdk_reports_requirement(self):
        with patch("importlib.metadata.version", side_effect=importlib.metadata.PackageNotFoundError):
            with self.assertRaisesRegex(SystemExit, "openevent-sdk>=0.8.0 must already be installed"):
                exec(self.preflight, {})
