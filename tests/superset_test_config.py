# -*- coding: utf-8 -*-
# flake8: noqa
from superset.config import *

CAS_AUTH = False
GUARDIAN_AUTH = False

LICENSE_CHECK = False
ENABLE_TIME_ROTATE = False

AUTH_USER_REGISTRATION_ROLE = 'alpha'
#SQLALCHEMY_DATABASE_URI = 'sqlite:///' + os.path.join(DATA_DIR, 'unittests.db')
SQLALCHEMY_DATABASE_URI = 'mysql://pilot:123456@172.16.130.109:3306/pilot_merge_superset?charset=utf8'

DEBUG = True

SUPERSET_WEBSERVER_PORT = 8081

# Allowing SQLALCHEMY_DATABASE_URI to be defined as an env var for
# continuous integration
if 'SUPERSET__SQLALCHEMY_DATABASE_URI' in os.environ:
    SQLALCHEMY_DATABASE_URI = os.environ.get('SUPERSET__SQLALCHEMY_DATABASE_URI')

SQL_CELERY_RESULTS_DB_FILE_PATH = os.path.join(DATA_DIR, 'celery_results.sqlite')
SQL_SELECT_AS_CTA = True
SQL_MAX_ROW = 666

TESTING = True
SECRET_KEY = 'thisismyscretkey'
WTF_CSRF_ENABLED = False
PUBLIC_ROLE_LIKE_GAMMA = True
AUTH_ROLE_PUBLIC = 'Public'
EMAIL_NOTIFICATIONS = False

CACHE_CONFIG = {'CACHE_TYPE': 'simple'}

class CeleryConfig(object):
    BROKER_URL = 'redis://localhost'
    CELERY_IMPORTS = ('superset.sql_lab', )
    CELERY_RESULT_BACKEND = 'db+sqlite:///' + SQL_CELERY_RESULTS_DB_FILE_PATH
    CELERY_ANNOTATIONS = {'sql_lab.add': {'rate_limit': '10/s'}}
    CONCURRENCY = 1


CELERY_CONFIG = CeleryConfig
