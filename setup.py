# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import json
import os
import subprocess

from setuptools import find_packages, setup

BASE_DIR = os.path.abspath(os.path.dirname(__file__))
PACKAGE_DIR = os.path.join(BASE_DIR, 'superset', 'static', 'assets')
PACKAGE_FILE = os.path.join(PACKAGE_DIR, 'package.json')
with open(PACKAGE_FILE) as package_file:
    version_string = json.load(package_file)['version']

with open('README.md') as readme:
    long_description = readme.read()


def get_git_sha():
    try:
        s = str(subprocess.check_output(['git', 'rev-parse', 'HEAD']))
        return s.strip()
    except Exception:
        return ''


GIT_SHA = get_git_sha()
version_info = {
    'GIT_SHA': GIT_SHA,
    'version': version_string,
}
print('-==-' * 15)
print('VERSION: ' + version_string)
print('GIT SHA: ' + GIT_SHA)
print('-==-' * 15)

with open(os.path.join(PACKAGE_DIR, 'version_info.json'), 'w') as version_file:
    json.dump(version_info, version_file)


setup(
    name='superset',
    description=(
        'A modern, enterprise-ready business intelligence web application'),
    long_description=long_description,
    version=version_string,
    packages=find_packages(),
    include_package_data=True,
    zip_safe=False,
    scripts=['superset/bin/superset'],
    install_requires=[
        'bleach==2.1.3',
        'boto3==1.4.7',
        'botocore==1.7.48',
        'celery==4.2.0',
        'colorama==0.3.9',
        'contextlib2==0.5.5',
        'cryptography==1.9',
        'cx_Oracle==5.3',
        'fileRobot-client==0.0.1',
        'flask-appbuilder==1.10.0',
        'flask-caching==1.4.0',
        'flask-compress==1.4.0',
        'flask-cors==3.0.3',
        'flask-migrate==2.1.1',
        'flask-restful==0.3.6',
        'flask-testing==0.7.1',
        'future==0.16.0',
        'geopy==1.15.0',
        'gunicorn==19.8.0',
        'humanize==0.5.1',
        'JPype1==0.6.2',
        'idna==2.6',
        'markdown==2.6.11',
        'mysqlclient==1.3.12',
        'pandas==0.23.3',
        'parsedatetime==2.0',
        'pydruid>=0.4.3',
        'pyhive==0.5.1',
        'pymssql==2.1.3',
        'pyodbc==4.0.11',
        'pathlib2==2.3.2',
        'polyline==1.3.2',
        'python-dateutil==2.6.1',
        'python-geohash==0.8.5',
        'pyyaml>=3.11',
        'requests==2.18.4',
        'simplejson==3.15.0',
        'sqlalchemy==1.2.2',
        'sqlalchemy-utils==0.32.21',
        'sqlparse==0.2.4',
        'tableschema==1.1.0',
        'thrift>=0.9.3',
        'thrift-sasl>=0.2.1',
        'unicodecsv==0.14.1',
        'unidecode>=0.04.21',
        'werkzeug==0.11.10',
        'xlrd==1.0.0',
        'xmltodict==0.11.0',
    ],
    extras_require={
        'cors': ['flask-cors>=2.0.0'],
        'console_log': ['console_log==0.2.10'],
    },
    author='Maxime Beauchemin',
    author_email='maximebeauchemin@gmail.com',
    url='https://github.com/apache/incubator-superset',
    download_url=(
        'https://github.com'
        '/apache/incubator-superset/tarball/' + version_string
    ),
    classifiers=[
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
    ],
)
