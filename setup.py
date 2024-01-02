#!/usr/bin/python3
# coding: utf-8
from setuptools import setup, find_packages


setup(
    name='osintflow',
    version='0.1.0-alpha',
    py_modules=['osintflow.concurrent', 'osintflow.config', 'osintflow.core', 'osintflow.util', 'osintflow.gdp'],
    requires=[
        'requests', 'pangres', 'pandas', 'mysql-connector-python', 'SQLAlchemy', 'setuptools', 'python-dateutil',
        'mysqlclient', 'chardet', 'xmltodict', 'PyYAML'
    ],
    author="Ngseo Kim",
    author_email="ngseo@s2w.inc",
    maintainer="DE team",
    url="https://github.com/Kim-Eungseo/osintflow",
    packages=find_packages(),
)
