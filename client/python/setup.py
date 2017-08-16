import sys
from setuptools import setup, find_packages

NAME = "swagger-client"
VERSION = "1.0.0"
REQUIRES = ["urllib3 >= 1.15", "six >= 1.10", "certifi", "python-dateutil"]

setup(
    name=NAME,
    version=VERSION,
    description="",
    install_requires=REQUIRES,
    packages=find_packages(),
    include_package_data=True
)
