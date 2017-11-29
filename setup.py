"""Minimal setup file for activity-history-conversion project."""

from setuptools import setup, find_packages

setup(
    name="activity_history_conversion",
    version="0.1.0",
    license="proprietary",
    description=(
        "Lambda-ready package to make Contact Note objects from "
        "Activity History and Event objects in Salesforce"
    ),

    author="Noble Network of Charter Schools",
    url="https://github.com/noblenetworkcharterschools/activity_history_conversion",

    packages=find_packages(where="src"),
    package_dir={"": "src"},

    install_requires=[],
)
