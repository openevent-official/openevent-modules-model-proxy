import csv
import io
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import unittest
import venv
from zipfile import ZipFile


ROOT = Path(__file__).resolve().parents[1]


def write_wheel(directory, name, version, files, requirements=()):
    """Create small installable fixtures, including RECORD for uninstall coverage."""
    directory.mkdir(parents=True, exist_ok=True)
    normalized = name.replace("-", "_")
    dist_info = f"{normalized}-{version}.dist-info"
    files = dict(files)
    files[f"{dist_info}/METADATA"] = (
        f"Metadata-Version: 2.1\nName: {name}\nVersion: {version}\n"
        + "".join(f"Requires-Dist: {requirement}\n" for requirement in requirements)
    )
    files[f"{dist_info}/WHEEL"] = (
        "Wheel-Version: 1.0\nRoot-Is-Purelib: true\nTag: py3-none-any\n"
    )
    record = io.StringIO()
    csv.writer(record).writerows((path, "", "") for path in (*files, f"{dist_info}/RECORD"))
    files[f"{dist_info}/RECORD"] = record.getvalue()
    wheel = directory / f"{normalized}-{version}-py3-none-any.whl"
    with ZipFile(wheel, "w") as archive:
        for path, content in files.items():
            archive.writestr(path, content)
    return wheel


class InstallTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        (self.root / "scripts").mkdir()
        shutil.copyfile(ROOT / "scripts" / "install.py", self.root / "scripts" / "install.py")
        self.env = dict(os.environ, PYTHONDONTWRITEBYTECODE="1", PIP_CONFIG_FILE=os.devnull,
                        PIP_DISABLE_PIP_VERSION_CHECK="1", PIP_CACHE_DIR=str(self.root / "cache"))
        self.env.pop("PYTHONPATH", None)
        for key in tuple(self.env):
            if key.startswith("PIP_") and key not in (
                "PIP_CONFIG_FILE", "PIP_DISABLE_PIP_VERSION_CHECK", "PIP_CACHE_DIR",
            ):
                self.env.pop(key)

    def make_environment(self):
        environment = self.root / "venv"
        venv.EnvBuilder(with_pip=True).create(environment)
        self.python = environment / ("Scripts/python.exe" if os.name == "nt" else "bin/python")
        result = self.run_python("-c", "import sysconfig; print(sysconfig.get_path('purelib'))")
        self.site = Path(result.stdout.strip())

    def run_python(self, *args, check=True, env=None):
        return subprocess.run([str(self.python), "-B", *map(str, args)],
                              env=env or self.env, text=True, capture_output=True, check=check)

    def install_old(self):
        dependency = write_wheel(self.root / "packages", "install-test-dependency", "1.0",
                                 {"install_test_dependency.py": "VALUE = 'keep me'\n"})
        project = write_wheel(self.root / "old", "openevent-model-proxy", "0.1.0",
                              {"install_test_project.py": "VALUE = 'old'\n",
                               "install_test_obsolete.py": ""})
        self.run_python("-m", "pip", "install", "--no-index", dependency, project)

    def install_new(self, requirements, env=None):
        write_wheel(self.root / "dist", "openevent-model-proxy", "0.1.0",
                    {"install_test_project.py": "VALUE = 'new'\n"}, requirements)
        return self.run_python(self.root / "scripts" / "install.py", "--no-index", "--find-links",
                               self.root / "packages", check=False, env=env)

    def test_same_version_replaces_code_removes_old_files_and_reuses_dependencies(self):
        self.make_environment()
        self.install_old()
        dependency_file = self.site / "install_test_dependency.py"
        dependency_file.write_text("VALUE = 'installed marker'\n")
        write_wheel(self.root / "packages", "install-test-dependency", "2.0",
                    {"install_test_dependency.py": "VALUE = 'should not upgrade'\n"})
        env = dict(self.env, PIP_FORCE_REINSTALL="1", PIP_IGNORE_INSTALLED="1", PIP_UPGRADE="1")
        result = self.install_new(("install-test-dependency>=1.0",
                                   'unavailable-test-extra; extra == "test"'), env=env)
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertEqual((self.site / "install_test_project.py").read_text(), "VALUE = 'new'\n")
        self.assertFalse((self.site / "install_test_obsolete.py").exists())
        self.assertEqual(dependency_file.read_text(), "VALUE = 'installed marker'\n")

    def test_missing_and_outdated_dependencies_are_installed(self):
        self.make_environment()
        self.install_old()
        write_wheel(self.root / "packages", "install-test-dependency", "2.0",
                    {"install_test_dependency.py": "VALUE = 'upgraded'\n"},
                    ("install-test-missing>=1.0",))
        write_wheel(self.root / "packages", "install-test-missing", "1.0",
                    {"install_test_missing.py": "VALUE = 'added'\n"})
        env = dict(self.env, PIP_NO_DEPS="1", PIP_NO_DEPENDENCIES="1")
        result = self.install_new(("install-test-dependency>=2.0",), env=env)
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertEqual((self.site / "install_test_dependency.py").read_text(), "VALUE = 'upgraded'\n")
        self.assertTrue((self.site / "install_test_missing.py").exists())

    def test_dependency_resolution_failure_preserves_installed_project(self):
        self.make_environment()
        self.install_old()
        result = self.install_new(("unavailable-install-test-dependency>=1.0",))
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("unavailable-install-test-dependency", result.stderr)
        self.assertEqual((self.site / "install_test_project.py").read_text(), "VALUE = 'old'\n")
        self.assertTrue((self.site / "install_test_obsolete.py").exists())

    def test_rejects_destination_and_dependency_policy_options(self):
        self.python = Path(sys.executable)
        for args in (("--target", str(self.root)), ("--user",), ("--upgrade",),
                     ("--force-reinstall",), ("--no-deps",)):
            with self.subTest(args=args):
                result = self.run_python(self.root / "scripts" / "install.py", *args, check=False)
                self.assertEqual(result.returncode, 2)
                self.assertIn("unrecognized arguments", result.stderr)
        for variable in ("PIP_TARGET", "PIP_PREFIX", "PIP_ROOT", "PIP_USER", "PIP_PYTHON"):
            with self.subTest(variable=variable):
                result = self.run_python(self.root / "scripts" / "install.py", check=False,
                                         env=dict(self.env, **{variable: "1"}))
                self.assertEqual(result.returncode, 2)
                self.assertIn(variable, result.stderr)

    def test_requires_exactly_one_project_wheel(self):
        self.python = Path(sys.executable)
        for versions in ((), ("0.1.0", "0.2.0")):
            with self.subTest(versions=versions):
                for version in versions:
                    write_wheel(self.root / "dist", "openevent-model-proxy", version, {})
                result = self.run_python(self.root / "scripts" / "install.py", check=False)
                self.assertEqual(result.returncode, 2)
                self.assertIn("exactly one", result.stderr)
