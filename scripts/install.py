"""Resolve wheel dependencies, then replace only this project's installed package."""

import argparse
from email.parser import BytesParser
import os
from pathlib import Path
import subprocess
import sys
from zipfile import ZipFile


def install(pip_args):
    parser = argparse.ArgumentParser(description=__doc__, allow_abbrev=False)
    for option in (
        "--extra-index-url", "--trusted-host",
        "--proxy", "--cert", "--client-cert", "--timeout", "--retries",
    ):
        parser.add_argument(option, action="append")
    parser.add_argument("-i", "--index-url", action="append")
    parser.add_argument("-f", "--find-links", action="append")
    for option in ("--no-index", "--disable-pip-version-check", "--break-system-packages"):
        parser.add_argument(option, action="store_true")
    parser.add_argument("-q", "--quiet", action="count")
    parser.add_argument("-v", "--verbose", action="count")
    parser.parse_args(pip_args)
    for variable in ("PIP_TARGET", "PIP_PREFIX", "PIP_ROOT", "PIP_PYTHON"):
        if os.environ.get(variable):
            parser.error(f"{variable} changes the installation environment; select it with PYTHON instead")
    if os.environ.get("PIP_USER", "").lower() not in ("", "0", "false", "no", "off"):
        parser.error("PIP_USER changes the installation environment; select it with PYTHON instead")

    root = Path(__file__).resolve().parents[1]
    wheels = list((root / "dist").glob("openevent_model_proxy-*.whl"))
    if len(wheels) != 1:
        parser.error("expected exactly one newly built openevent-model-proxy wheel in dist/")
    wheel = wheels[0]
    with ZipFile(wheel) as archive:
        metadata_path, = (name for name in archive.namelist() if name.endswith(".dist-info/METADATA"))
        requirements = BytesParser().parsebytes(archive.read(metadata_path)).get_all("Requires-Dist", [])

    work = root / "build" / "install"
    work.mkdir(parents=True, exist_ok=True)
    env = dict(os.environ, PYTHONDONTWRITEBYTECODE="1", TMPDIR=str(work),
               PIP_CACHE_DIR=str(root / "build" / "pip-cache"),
               PIP_FORCE_REINSTALL="false", PIP_IGNORE_INSTALLED="false", PIP_UPGRADE="false",
               PIP_NO_DEPS="false", PIP_NO_DEPENDENCIES="false")
    pip = [sys.executable, "-B", "-m", "pip", "install", *pip_args]
    if requirements:
        requirement_file = work / "requirements.txt"
        requirement_file.write_text("\n".join(requirements) + "\n", encoding="utf-8")
        subprocess.run([*pip, "--requirement", str(requirement_file)], check=True, env=env)
    subprocess.run([*pip, "--force-reinstall", "--no-deps", str(wheel)], check=True, env=env)


if __name__ == "__main__":
    install(sys.argv[1:])
