"""The main config file for Superset

All configuration in this file can be overridden by providing a pilot
in your PYTHONPATH as there is a ``from pilot import *``
at the end of this file.
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import imp
import json
import os
import sys

from flask_appbuilder.security.manager import AUTH_DB
from superset.stats_logger import DummyStatsLogger

# Realtime stats logger, a StatsD implementation exists
STATS_LOGGER = DummyStatsLogger()

BASE_DIR = os.path.abspath(os.path.dirname(__file__))
DATA_DIR = os.path.join(os.path.expanduser('~'), 'pilot')
# if not os.path.exists(DATA_DIR):
#     os.makedirs(DATA_DIR)

# ---------------------------------------------------------
# Pilot specific config
# ---------------------------------------------------------
PACKAGE_DIR = os.path.join(BASE_DIR, 'static', 'assets')
PACKAGE_FILE = os.path.join(PACKAGE_DIR, 'package.json')
with open(PACKAGE_FILE) as package_file:
    VERSION_STRING = json.load(package_file)['version']

PILOT_WORKERS = 2
PILOT_WEBSERVER_ADDRESS = '0.0.0.0'
PILOT_WEBSERVER_PORT = 8086
PILOT_WEBSERVER_TIMEOUT = 600

CUSTOM_SECURITY_MANAGER = None
SQLALCHEMY_TRACK_MODIFICATIONS = False
# ---------------------------------------------------------

# The default username and password when guardian is not opened
DEFAULT_USERNAME = 'admin'
DEFAULT_PASSWORD = '123456'

# The Community Edition will abandon guardian module, and embed user management module
COMMUNITY_EDITION = False
COMMUNITY_USERNAME = DEFAULT_USERNAME
COMMUNITY_PASSWORD = DEFAULT_PASSWORD

# Guardian
GUARDIAN_AUTH = True
GUARDIAN_SERVER = 'https://localhost:8380'  # Used for proxy ticket and access token
GUARDIAN_SERVICE_TYPE = 'PILOT'
GUARDIAN_ACCESS_TOKEN_NAME = 'pilot-token'

STUDIO_ADMIN_ROLE_NAME = 'Admin'
STUDIO_DEVELOPER_ROLE_NAME = 'Developer'
STUDIO_VIEWER_ROLE_NAME = 'Viewer'

# CAS
CAS_AUTH = False
CAS_SERVER = 'https://localhost:8393'
CAS_URL_PREFIX = '/cas'

# if load examples data when start server
LOAD_EXAMPLES = True

# License check
LICENSE_CHECK = True

# Your App secret key
SECRET_KEY = '=== Transwarp Studio Pilot ==='  # noqa

# Session timeout
PERMANENT_SESSION_LIFETIME = 86400


METADATA_CONN_NAME = 'main'

# The SQLAlchemy connection string.
SQLALCHEMY_DATABASE_URI = 'mysql://username:password@localhost:3306/test?charset=utf8'

# Default Inceptor
DEFAULT_INCEPTOR_CONN_NAME = 'default_inceptor'
DEFAULT_INCEPTOR_SERVER = 'node01:10000'  # should be <node01>[,node02]:<port>

# Default Hdfs
DEFAULT_HDFS_CONN_NAME = 'default_hdfs'
DEFAULT_HTTPFS = 'localhost'

# Filerobot microservice
FILE_ROBOT_SERVER = 'localhost:5005'


# Timeout for database or hdfs connection
CONNECTION_TIMEOUT = 60

# The limit of queries fetched for query search
QUERY_SEARCH_LIMIT = 100

# The limit of rows of slice
SLICE_ROW_LIMIT = 2000


# Flask-WTF flag for CSRF
WTF_CSRF_ENABLED = True

# Add endpoints that need to be exempt from CSRF protection
WTF_CSRF_EXEMPT_LIST = []

# Whether to run the web server in debug mode or not
DEBUG = False
FLASK_USE_RELOAD = True

# Whether to show the stacktrace on 500 error
SHOW_STACKTRACE = True

# Extract and use X-Forwarded-For/X-Forwarded-Proto headers?
ENABLE_PROXY_FIX = False

# ------------------------------
# GLOBALS FOR APP Builder
# ------------------------------
# Uncomment to setup Your App name
APP_NAME = "PILOT"
COPYRIGHT = "@2017 TRANSWARP ALL Rights Reserved"

# Uncomment to setup an App icon
APP_ICON = "/static/assets/images/superset-logo@2x.png"


# ----------------------------------------------------
# AUTHENTICATION CONFIG
# ----------------------------------------------------
# The authentication type
# AUTH_OID : Is for OpenID
# AUTH_DB : Is for database (username/password()
# AUTH_LDAP : Is for LDAP
# AUTH_REMOTE_USER : Is for using REMOTE_USER from web server
AUTH_TYPE = AUTH_DB

# ---------------------------------------------------
# Babel config for translations
# ---------------------------------------------------
# Setup default language
BABEL_DEFAULT_LOCALE = 'zh'
# Your application default translation path
BABEL_DEFAULT_FOLDER = 'superset/translations'
# The allowed translation for you app
LANGUAGES = {
    'en': {'flag': 'us', 'name': 'English'},
    # 'it': {'flag': 'it', 'name': 'Italian'},
    # 'fr': {'flag': 'fr', 'name': 'French'},
    'zh': {'flag': 'cn', 'name': 'Chinese'},
    # 'ja': {'flag': 'jp', 'name': 'Japanese'},
    # 'de': {'flag': 'de', 'name': 'German'},
    # 'pt_BR': {'flag': 'br', 'name': 'Brazilian Portuguese'},
    # 'ru': {'flag': 'ru', 'name': 'Russian'},
}

# ---------------------------------------------------
# Image and file configuration
# ---------------------------------------------------
# The file upload folder, when using models with files
UPLOAD_FOLDER = BASE_DIR + '/app/static/uploads/'

# The image upload folder, when using models with images
IMG_UPLOAD_FOLDER = BASE_DIR + '/app/static/uploads/'

# The image upload url, when using models with images
IMG_UPLOAD_URL = '/static/uploads/'
# Setup image size default is (300, 200, True)
# IMG_SIZE = (300, 200, True)

# The max content length of HTTP request Flask received
MAX_CONTENT_LENGTH = 4 * 1024 * 1024 * 1024

# The length of file block for uploading or downloading
FILE_BLOCK_LENGTH = 64 * 1024 * 1024

# Max size of downloaded file in HDFS
MAX_DOWNLOAD_SIZE = 512 * 1024 * 1024

# Global folder for slice cache, keytab, cas file
GLOBAL_FOLDER = '/tmp/pilot'

CACHE_DEFAULT_TIMEOUT = 86400
CACHE_CONFIG = {'CACHE_TYPE': 'filesystem',
                'CACHE_THRESHOLD': 500,
                'CACHE_DIR': '{}/cache'.format(GLOBAL_FOLDER)}
TABLE_NAMES_CACHE_CONFIG = {'CACHE_TYPE': 'null'}

# CORS Options
ENABLE_CORS = False
CORS_OPTIONS = {}

# Allowed format types for upload on Database view
# TODO: Add processing of other spreadsheet formats (xls, xlsx etc)
ALLOWED_EXTENSIONS = set(['csv'])

# CSV Options: key/value pairs that will be passed as argument to DataFrame.to_csv method
# note: index option should not be overridden
CSV_EXPORT = {
    'encoding': 'utf-8',
}

# ---------------------------------------------------
# List of viz_types not allowed in your environment
# For example: Blacklist pivot table and treemap:
#  VIZ_TYPE_BLACKLIST = ['pivot_table', 'treemap']
# ---------------------------------------------------
VIZ_TYPE_BLACKLIST = []

# ---------------------------------------------------
# List of data sources not to be refreshed in druid cluster
# ---------------------------------------------------
DRUID_DATA_SOURCE_BLACKLIST = []

# --------------------------------------------------
# Modules, datasources and middleware to be registered
# --------------------------------------------------
DEFAULT_MODULE_DS_MAP = {'superset.models.dataset': ['Dataset']}
ADDITIONAL_MODULE_DS_MAP = {}
ADDITIONAL_MIDDLEWARE = []

"""
1) http://docs.python-guide.org/en/latest/writing/logging/
2) https://docs.python.org/2/library/logging.config.html
"""

# Console Log Settings

LOG_FORMAT = '%(asctime)s:%(levelname)-8s:%(name)s:%(filename)s %(funcName)s(): %(message)s'
LOG_LEVEL = 'INFO'

# ---------------------------------------------------
# Enable Time Rotate Log Handler
# ---------------------------------------------------
# LOG_LEVEL = DEBUG, INFO, WARNING, ERROR, CRITICAL
ENABLE_TIME_ROTATE = True
TIME_ROTATE_LOG_LEVEL = 'INFO'
FILENAME = '/var/log/pilot/pilot.log'
ROLLOVER = 'midnight'
INTERVAL = 1
BACKUP_COUNT = 30

# Set this API key to enable Mapbox visualizations
MAPBOX_API_KEY = "pk.eyJ1IjoiemhhbmdqaWFqaWUiLCJhIjoiY2o0NnFzb29hMDNzZTMzbzE0a2lrd2FvZiJ9.GajDhKuG9zZb2_g0DaEtJw"


# ---------------------------------------------------
# SQL Lab
# ---------------------------------------------------
# Timeout duration for SQL Lab synchronous queries
SQLLAB_TIMEOUT = 300

# Maximum number of rows returned in the SQL editor
SQL_MAX_ROW = 20

# The MAX duration (in seconds) a query can run for before being killed
# by celery.
SQLLAB_ASYNC_TIME_LIMIT_SEC = 60 * 60 * 6

# Maximum number of tables/views displayed in the dropdown window in SQL Lab.
MAX_TABLE_NAMES = 3000

# An instantiated derivative of werkzeug.contrib.cache.BaseCache
# if enabled, it can be used to store the results of long-running queries
# in SQL Lab by using the "Run Async" button/feature
RESULTS_BACKEND = None


# If defined, shows this text in an alert-warning box in the navbar
# one example use case may be "STAGING" to make it clear that this is
# not the production version of the site.
WARNING_MSG = None

# Default celery config is to use SQLA as a broker, in a production setting
# you'll want to use a proper broker as specified here:
# http://docs.celeryproject.org/en/latest/getting-started/brokers/index.html
"""
# Example:
class CeleryConfig(object):
  BROKER_URL = 'sqla+sqlite:///celerydb.sqlite'
  CELERY_IMPORTS = ('superset.sql_lab', )
  CELERY_RESULT_BACKEND = 'db+sqlite:///celery_results.sqlite'
  CELERY_ANNOTATIONS = {'tasks.add': {'rate_limit': '10/s'}}
CELERY_CONFIG = CeleryConfig
"""
CELERY_CONFIG = None
SQL_CELERY_DB_FILE_PATH = os.path.join(DATA_DIR, 'celerydb.sqlite')
SQL_CELERY_RESULTS_DB_FILE_PATH = os.path.join(DATA_DIR, 'celery_results.sqlite')

# static http headers to be served by your Superset server.
# This header prevents iFrames from other domains and
# "clickjacking" as a result
HTTP_HEADERS = {'X-Frame-Options': 'SAMEORIGIN'}
# If you need to allow iframes from other domains (and are
# aware of the risks), you can disable this header:
# HTTP_HEADERS = {}

# The S3 bucket where you want to store your external hive tables created
# from CSV files. For example, 'companyname-superset'
CSV_TO_HIVE_UPLOAD_S3_BUCKET = None

# The directory within the bucket specified above that will
# contain all the external tables
CSV_TO_HIVE_UPLOAD_DIRECTORY = 'EXTERNAL_HIVE_TABLES/'

# The namespace within hive where the tables created from
# uploading CSVs will be stored.
UPLOADED_CSV_HIVE_NAMESPACE = None

# A dictionary of items that gets merged into the Jinja context for
# SQL Lab. The existing context gets updated with this dictionary,
# meaning values for existing keys get overwritten by the content of this
# dictionary.
JINJA_CONTEXT_ADDONS = {}

# Roles that are controlled by the API / Superset and should not be changes
# by humans.
ROBOT_PERMISSION_ROLES = ['Public', 'Gamma', 'Alpha', 'Admin', 'sql_lab']


CONFIG_PATH_ENV_VAR = 'PILOT_CONFIG_PATH'

# If a callable is specified, it will be called at app startup while passing
# a reference to the Flask app. This can be used to alter the Flask app
# in whatever way.
# example: FLASK_APP_MUTATOR = lambda x: x.before_request = f
FLASK_APP_MUTATOR = None

# Set this to false if you don't want users to be able to request/grant
# datasource access requests from/to other users.
ENABLE_ACCESS_REQUEST = False


# smtp server configuration
EMAIL_NOTIFICATIONS = False  # all the emails are sent using dryrun
SMTP_HOST = 'localhost'
SMTP_STARTTLS = True
SMTP_SSL = False
SMTP_USER = 'pilot'
SMTP_PORT = 25
SMTP_PASSWORD = 'pilot'
SMTP_MAIL_FROM = 'pilot@pilot.com'

if not CACHE_DEFAULT_TIMEOUT:
    CACHE_DEFAULT_TIMEOUT = CACHE_CONFIG.get('CACHE_DEFAULT_TIMEOUT')

# Whether to bump the logging level to ERRROR on the flask_appbiulder package
# Set to False if/when debugging FAB related issues like
# permission management
SILENCE_FAB = True

# The link to a page containing common errors and their resolutions
# It will be appended at the bottom of sql_lab errors.
TROUBLESHOOTING_LINK = ''

# CSRF token timeout, set to None for a token that never expires
WTF_CSRF_TIME_LIMIT = 60 * 60 * 24 * 7

# This link should lead to a page with instructions on how to gain access to a
# Datasource. It will be placed at the bottom of permissions errors.
PERMISSION_INSTRUCTIONS_LINK = ''

# Integrate external Blueprints to the app by passing them to your
# configuration. These blueprints will get integrated in the app
BLUEPRINTS = []

# Provide a callable that receives a tracking_url and returns another
# URL. This is used to translate internal Hadoop job tracker URL
# into a proxied one
TRACKING_URL_TRANSFORMER = lambda x: x  # noqa: E731

# Interval between consecutive polls when using Hive Engine
HIVE_POLL_INTERVAL = 5

# Allow for javascript controls components
# this enables programmers to customize certain charts (like the
# geospatial ones) by inputing javascript in controls. This exposes
# an XSS security vulnerability
ENABLE_JAVASCRIPT_CONTROLS = False

# A callable that allows altering the database conneciton URL and params
# on the fly, at runtime. This allows for things like impersonation or
# arbitrary logic. For instance you can wire different users to
# use different connection parameters, or pass their email address as the
# username. The function receives the connection uri object, connection
# params, the username, and returns the mutated uri and params objects.
# Example:
#   def DB_CONNECTION_MUTATOR(uri, params, username, security_manager):
#       user = security_manager.find_user(username=username)
#       if user and user.email:
#           uri.username = user.email
#       return uri, params
#
# Note that the returned uri and params are passed directly to sqlalchemy's
# as such `create_engine(url, **params)`
DB_CONNECTION_MUTATOR = None

# A function that intercepts the SQL to be executed and can alter it.
# The use case is can be around adding some sort of comment header
# with information such as the username and worker node information
#
#    def SQL_QUERY_MUTATOR(sql, username, security_manager):
#        dttm = datetime.now().isoformat()
#        return "-- [SQL LAB] {username} {dttm}\n sql"(**locals())
SQL_QUERY_MUTATOR = None

# When not using gunicorn, (nginx for instance), you may want to disable
# using flask-compress
ENABLE_FLASK_COMPRESS = True

# Dashboard v1 deprecation configuration
DASH_V2_IS_DEFAULT_VIEW_FOR_EDITORS = True
CAN_FALLBACK_TO_DASH_V1_EDIT_MODE = True
# these are incorporated into messages displayed to users
PLANNED_V2_AUTO_CONVERT_DATE = None  # e.g. '2018-06-16'
V2_FEEDBACK_URL = None  # e.g., 'https://goo.gl/forms/...'


try:
    if CONFIG_PATH_ENV_VAR in os.environ:
        # Explicitly import config module that is not in pythonpath; useful
        # for case where app is being executed via pex.
        print('Loaded your LOCAL configuration at [{}]'.format(
            os.environ[CONFIG_PATH_ENV_VAR]))
        module = sys.modules[__name__]
        override_conf = imp.load_source(
            'pilot_config',
            os.environ[CONFIG_PATH_ENV_VAR])
        for key in dir(override_conf):
            if key.isupper():
                setattr(module, key, getattr(override_conf, key))

    else:
        from pilot_config import *  # noqa
        import pilot_config
        print('Loaded your LOCAL configuration at [{}]'.format(pilot_config.__file__))
except ImportError:
    pass
