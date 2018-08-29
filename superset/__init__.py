"""Package's main module!"""
import flask
import json
import logging
import os
import ssl
from logging.handlers import TimedRotatingFileHandler

from sqlalchemy_utils.functions import database_exists, create_database
from flask import g, Flask, redirect
from flask_appbuilder import SQLA, AppBuilder, IndexView
from flask_appbuilder.baseviews import expose
from flask_caching import Cache
from flask_migrate import Migrate
from flask_compress import Compress
from superset.cas import CAS, login_required
from superset.cas.cas_session import cas_session
from superset.cas.keys import (
    CAS_FAKE_SERVICE_TICKET, CAS_FAKE_USERNAME, CAS_SERVICE_TICKET, CAS_USERNAME
)
from superset.connector_registry import ConnectorRegistry
from werkzeug.contrib.fixers import ProxyFix
from superset import utils, config
from superset.jvm import start_jvm, shutdown_jvm
from superset.check_license import check_license
from superset.security import SupersetSecurityManager


APP_DIR = os.path.dirname(__file__)
CONFIG_MODULE = os.environ.get('SUPERSET_CONFIG', 'superset.config')

with open(APP_DIR + '/static/assets/backendSync.json', 'r') as f:
    frontend_config = json.load(f)

app = Flask(__name__)
app.config.from_object(CONFIG_MODULE)
app.config['TEMPLATES_AUTO_RELOAD'] = True
Compress(app)

conf = app.config

#################################################################
# Handling manifest file logic at app start
#################################################################
MANIFEST_FILE = APP_DIR + '/static/assets/dist/manifest.json'
FEATURE_GOGGLES = APP_DIR + '/static/assets/featureToggles.json'
manifest = {}


def parse_manifest_json():
    global manifest
    try:
        with open(MANIFEST_FILE, 'r') as f:
            manifest = json.load(f)
    except Exception:
        pass


def parse_feature_toggles():
    global toggles
    try:
        with open(FEATURE_GOGGLES, 'r') as f:
            toggles = json.load(f)
    except Exception:
        pass


def get_manifest_file(filename):
    if app.debug:
        parse_manifest_json()
    return '/static/assets/dist/' + manifest.get(filename, '')


parse_manifest_json()
parse_feature_toggles()


@app.context_processor
def get_js_manifest():
    return dict(js_manifest=get_manifest_file)


@app.context_processor
def get_feature_toggles():
    t = json.dumps(toggles)
    return dict(feature_toggles=t)

#################################################################

# CAS
cas = None
if conf.get('CAS_AUTH'):
    cas = CAS(app, conf.get('CAS_URL_PREFIX'))
ssl._create_default_https_context = ssl._create_unverified_context


for bp in conf.get('BLUEPRINTS'):
    try:
        print("Registering blueprint: '{}'".format(bp.name))
        app.register_blueprint(bp)
    except Exception as e:
        print('blueprint registration failed')
        logging.exception(e)

if conf.get('SILENCE_FAB'):
    logging.getLogger('flask_appbuilder').setLevel(logging.ERROR)

if app.debug:
    # In production mode, add log handler to sys.stderr.
    app.logger.addHandler(logging.StreamHandler())
    app.logger.setLevel(logging.INFO)


if not database_exists(conf.get("SQLALCHEMY_DATABASE_URI")):
    print("Create database ...")
    create_database(conf.get("SQLALCHEMY_DATABASE_URI"), "utf8")

db = SQLA(app)


utils.pessimistic_connection_handling(db.engine.pool)

# cache for slice data
file_cache = Cache(app, config=app.config.get('CACHE_CONFIG'))

# simple cache for share data among threads
simple_cache = Cache(app, config={'CACHE_TYPE': 'simple',
                                  'CACHE_DEFAULT_TIMEOUT': 30 * 86400})
tables_cache = utils.setup_cache(app, conf.get('TABLE_NAMES_CACHE_CONFIG'))


migrate = Migrate(app, db, directory=APP_DIR + "/migrations")

logging.getLogger('flask_appbuilder').setLevel(logging.WARNING)

# Logging configuration
logging.basicConfig(format=app.config.get('LOG_FORMAT'))
logging.getLogger().setLevel(app.config.get('LOG_LEVEL'))

if app.config.get('ENABLE_TIME_ROTATE'):
    handler = TimedRotatingFileHandler(app.config.get('FILENAME'),
                                       when=app.config.get('ROLLOVER'),
                                       interval=app.config.get('INTERVAL'),
                                       backupCount=app.config.get('BACKUP_COUNT'))
    handler.setFormatter(logging.Formatter(app.config.get('LOG_FORMAT')))
    handler.setLevel(app.config.get('LOG_LEVEL'))
    logging.getLogger().addHandler(handler)


if conf.get('LICENSE_CHECK') or conf.get('GUARDIAN_AUTH'):
    start_jvm()


if conf.get('LICENSE_CHECK'):
    check_license()


if not conf.get('GUARDIAN_AUTH'):
    shutdown_jvm()


if app.config.get('ENABLE_CORS'):
    from flask_cors import CORS
    CORS(app, **app.config.get('CORS_OPTIONS'))


if app.config.get('ENABLE_PROXY_FIX'):
    app.wsgi_app = ProxyFix(app.wsgi_app)


if app.config.get('ENABLE_CHUNK_ENCODING'):

    class ChunkedEncodingFix(object):
        def __init__(self, app):
            self.app = app

        def __call__(self, environ, start_response):
            # Setting wsgi.input_terminated tells werkzeug.wsgi to ignore
            # content-length and read the stream till the end.
            if environ.get('HTTP_TRANSFER_ENCODING', '').lower() == u'chunked':
                environ['wsgi.input_terminated'] = True
            return self.app(environ, start_response)

    app.wsgi_app = ChunkedEncodingFix(app.wsgi_app)


if app.config.get('UPLOAD_FOLDER'):
    try:
        os.makedirs(app.config.get('UPLOAD_FOLDER'))
    except OSError:
        pass


for middleware in app.config.get('ADDITIONAL_MIDDLEWARE'):
    app.wsgi_app = middleware(app.wsgi_app)


def index_view():
    if conf.get('CAS_AUTH'):
        class MyIndexView(IndexView):
            @expose('/')
            @login_required
            def index(self):
                ### login in appbuilder
                # import flask
                # data = json.dumps({'username': cas.username, 'password': '123456'})
                # flask.session['user'] = data
                # return redirect(flask.url_for('AuthDBView.login'))

                ### login here
                utils.login_app(appbuilder, cas.username, conf.get('DEFAULT_PASSWORD'))
                url = self.get_redirect()
                if url == self.appbuilder.get_url_for_index:
                    url = '/home'
                return redirect(url)
    else:
        class MyIndexView(IndexView):
            @expose('/')
            def index(self):
                url = self.get_redirect()
                if url == self.appbuilder.get_url_for_index:
                    url = '/home'
                return redirect(url)
    return MyIndexView


custom_sm = app.config.get('CUSTOM_SECURITY_MANAGER') or SupersetSecurityManager
if not issubclass(custom_sm, SupersetSecurityManager):
    raise Exception(
        """Your CUSTOM_SECURITY_MANAGER must now extend SupersetSecurityManager,
         not FAB's security manager.
         See [4565] in UPDATING.md""")

appbuilder = AppBuilder(
    app, db.session,
    base_template='superset/base.html',
    indexview=index_view(),
    security_manager_class=custom_sm,
    update_perms=utils.get_update_perms_flag(),
)

security_manager = appbuilder.sm

get_session = appbuilder.get_session
results_backend = app.config.get("RESULTS_BACKEND")

# Registering sources
module_datasource_map = app.config.get("DEFAULT_MODULE_DS_MAP")
module_datasource_map.update(app.config.get("ADDITIONAL_MODULE_DS_MAP"))
ConnectorRegistry.register_sources(module_datasource_map)

from superset import views, config  # noqa


@app.before_request
def before_request():
    """Used for CAS Single Logout
    """
    if app.config.get("CAS_AUTH") is True:
        if CAS_SERVICE_TICKET not in flask.session \
                or CAS_USERNAME not in flask.session:
            st = CAS_FAKE_SERVICE_TICKET
            flask.session[CAS_SERVICE_TICKET] = st
            cas_session.record(st, CAS_FAKE_USERNAME, clear_logout=False)
            cas_session.logout_st(st)
            cas_session.verify_st(st)

        st = flask.session.get(CAS_SERVICE_TICKET)
        if cas_session.is_logout_st(st):
            if cas_session.is_verified_st(st):
                pass
            else:
                cas_session.verify_st(st)
                return flask.redirect(appbuilder.get_url_for_logout)

