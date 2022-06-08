"""A setuptools based setup module.
See:
https://packaging.python.org/guides/distributing-packages-using-setuptools/
https://github.com/pypa/sampleproject
"""

# Always prefer setuptools over distutils
from setuptools import setup, find_packages
import pathlib

install_cmd = None
try:
    from install_cmd import InstallWrapper

    install_cmd = InstallWrapper
except ModuleNotFoundError:
    pass

here = pathlib.Path(__file__).parent.resolve()

# Get the long description from the README file
long_description = (here / "README.md").read_text(encoding="utf-8")


extra = dict()

if install_cmd:
    cmdclass = {"install": install_cmd}
    extra["cmdclass"] = cmdclass

setup(
    url="https://github.com/jheld/diycrate",
    test_suite="tests",
    packages=find_packages(exclude=["tests", "tests.*"]),
    long_description=long_description,
    long_description_content_type="text/markdown",
    **extra,
)
