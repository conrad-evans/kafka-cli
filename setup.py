import os
from setuptools import setup, find_packages

setup(
    name="kafka-cli",
    version="1.0.0",
    description="Kafka CLI integration in python",
    long_description=open(
        os.path.join(os.path.abspath(os.path.dirname(__file__)), "README.md")
    ).read(),
    long_description_content_type="text/markdown",
    author="conrad mugabe",
    packages=find_packages(),
)
