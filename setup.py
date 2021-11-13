#!/usr/bin/env python
from setuptools import setup

setup(
    name="tap-instagram",
    version="0.1.0",
    description="Singer.io tap for extracting data",
    author="Stitch",
    url="http://singer.io",
    classifiers=["Programming Language :: Python :: 3 :: Only"],
    py_modules=["tap_instagram"],
    install_requires=[
        # NB: Pin these to a more specific version for tap reliability
        "singer-python==5.8.0",
        "requests==2.20.0",
        "facebook-business==12.0.0",
        "backoff==1.8.0",
        "cached-property==1.5.2",
        "pendulum==2.1.2"
    ],
    entry_points="""
    [console_scripts]
    tap-instagram=tap_instagram:main
    """,
    packages=["tap_instagram"],
    package_data = {
        "schemas": ["tap_instagram/schemas/*.json"]
    },
    include_package_data=True,
)
