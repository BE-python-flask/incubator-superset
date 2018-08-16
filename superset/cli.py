#!/usr/bin/env python
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

from datetime import datetime
import os
import logging
from colorama import Fore, Style
from flask_migrate import MigrateCommand, upgrade
from flask_script import Manager
from pathlib2 import Path
from subprocess import Popen
from sys import stdout
import werkzeug.serving
import yaml

from superset import app, db, data, security, dict_import_export_util, utils
from superset import security_manager as sm
from superset.models import HDFSConnection, Log, Database


config = app.config
celery_app = utils.get_celery_app(config)

manager = Manager(app)
manager.add_command('db', MigrateCommand)


@manager.command
def init():
    """Inits the application"""
    security.sync_role_definitions()


def init_tables_and_roles():
    logging.info("Start to upgrade metadata tables...")
    BASE_DIR = os.path.abspath(os.path.dirname(__file__))
    migration_dir = os.path.join(BASE_DIR, 'migrations')
    upgrade(directory=migration_dir)
    db.session.commit()
    logging.info("Finish to upgrade metadata tables.")


def init_examples():
    if config.get('LOAD_EXAMPLES'):
        rs = db.session.execute('show tables like "energy_usage";')
        if rs.rowcount == 0:
            logging.info("Start to load examples data...")
            load_examples(False, user_id=None)
            logging.info("Finish to load examples data.")
        else:
            logging.info("Exists examples data (such as: energy_usage).")


def create_default_user():
    if config.get('GUARDIAN_AUTH', False):
        return
    elif config.get('COMMUNITY_EDITION'):
        username = config.get('COMMUNITY_USERNAME')
        password = config.get('COMMUNITY_PASSWORD')
    else:
        username = config.get('DEFAULT_USERNAME')
        password = config.get('DEFAULT_PASSWORD')

    user = sm.find_user(username=username)
    if not user:
        logging.info("Begin to create default admin user...")
        user = sm.add_user(
            username, username, username,
            '{}@email.com'.format(username),
            sm.find_role('Admin'),
            password=password)
        if not user:
            logging.error("Failed to add default admin user.")
    sm.reset_password(user.id, password)
    user.password2 = password
    sm.get_session.commit()
    logging.info("Finish to add or edit default admin user.")


def create_default_inceptor_conn():
    name = config.get('DEFAULT_INCEPTOR_CONN_NAME')
    database = db.session.query(Database).filter_by(database_name=name).first()
    uri = 'inceptor://{}/default'.format(config.get('DEFAULT_INCEPTOR_SERVER'))
    if database:
        database.sqlalchemy_uri = uri
        db.session.commit()
        logging.info("Success to edit default inceptor connection.")
        Log.log_update(database, 'database', None)
    else:
        database = Database(database_name=name,
                            sqlalchemy_uri=uri,
                            args='{"connect_args": {"hive": "Hive Server 2", "mech": "Token"}}',
                            description='Default inceptor connection.')
        db.session.add(database)
        db.session.commit()
        logging.info("Success to add default inceptor connection.")
        Log.log_add(database, 'database', None)


def create_default_hdfs_conn():
    name = config.get('DEFAULT_HDFS_CONN_NAME')
    hconn = db.session.query(HDFSConnection).filter_by(connection_name=name).first()
    if hconn:
        hconn.httpfs = config.get('DEFAULT_HTTPFS')
        db.session.add(hconn)
        db.session.commit()
        logging.info("Success to edit default hdfs connection.")
        Log.log_update(hconn, 'hdfsconnection', None)
    else:
        hconn = HDFSConnection(connection_name=name,
                               httpfs=config.get('DEFAULT_HTTPFS'),
                               online=True,
                               description='Default hdfs connection for hdfs browser.')
        db.session.add(hconn)
        db.session.commit()
        logging.info("Success to add default hdfs connection.")
        Log.log_add(hconn, 'hdfsconnection', None)


def register_in_guardian():
    if config.get('GUARDIAN_AUTH'):
        from superset.guardian import guardian_client
        guardian_client.login()
        guardian_client.register()
        logging.info("Finish to register service in Guardian")


def init_studio_role():
    if config.get('GUARDIAN_AUTH') is True:
        from superset.guardian import guardian_client, guardian_admin
        admin_role = config.get('STUDIO_ADMIN_ROLE_NAME')
        if admin_role and guardian_client.get_role(admin_role):
            logging.info('Grant role [{}] global admin permission'.format(admin_role))
            guardian_admin.grant_admin_role(admin_role)

        developer_role = config.get('STUDIO_DEVELOPER_ROLE_NAME')
        if developer_role and guardian_client.get_role(developer_role):
            logging.info('Grant role [{}] global developer permission'.format(developer_role))
            guardian_admin.grant_developer_role(developer_role)

        viewer_role = config.get('STUDIO_VIEWER_ROLE_NAME')
        if viewer_role and guardian_client.get_role(viewer_role):
            logging.info('Grant role [{}] global viewer permission'.format(viewer_role))
            guardian_admin.grant_viewer_role(viewer_role)


def init_pilot():
    register_in_guardian()
    init_studio_role()
    # init_tables_and_roles()
    create_default_user()
    init_examples()
    create_default_inceptor_conn()
    create_default_hdfs_conn()


def debug_run(app, port, use_reloader):
    return app.run(
        host='0.0.0.0',
        port=int(port),
        threaded=True,
        debug=True,
        use_reloader=use_reloader)


def console_log_run(app, port, use_reloader):
    # from console_log import ConsoleLog
    # from gevent import pywsgi
    # from geventwebsocket.handler import WebSocketHandler
    #
    # app.wsgi_app = ConsoleLog(app.wsgi_app, app.logger)
    #
    # def run():
    #     server = pywsgi.WSGIServer(
    #         ('0.0.0.0', int(port)),
    #         app,
    #         handler_class=WebSocketHandler)
    #     server.serve_forever()
    #
    # if use_reloader:
    #     from gevent import monkey
    #     monkey.patch_all()
    #     run = werkzeug.serving.run_with_reloader(run)
    #
    # run()
    pass


@manager.option(
    '-d', '--debug', action='store_true',
    help='Start the web server in debug mode')
@manager.option(
    '--console-log', action='store_true',
    help='Create logger that logs to the browser console (implies -d)')
@manager.option(
    '-n', '--no-reload', action='store_false', dest='use_reloader',
    default=config.get('FLASK_USE_RELOAD'),
    help="Don't use the reloader in debug mode")
@manager.option(
    '-a', '--address', default=config.get('PILOT_WEBSERVER_ADDRESS'),
    help='Specify the address to which to bind the web server')
@manager.option(
    '-p', '--port', default=config.get('PILOT_WEBSERVER_PORT'),
    help='Specify the port on which to run the web server')
@manager.option(
    '-w', '--workers',
    default=config.get('PILOT_WORKERS', 2),
    help='Number of gunicorn web server workers to fire up [DEPRECATED]')
@manager.option(
    '-t', '--timeout', default=config.get('PILOT_WEBSERVER_TIMEOUT'),
    help='Specify the timeout (seconds) for the gunicorn web server [DEPRECATED]')
@manager.option(
    '-s', '--socket', default=config.get('PILOT_WEBSERVER_SOCKET'),
    help='Path to a UNIX socket as an alternative to address:port, e.g. '
         '/var/run/superset.sock. '
         'Will override the address and port values. [DEPRECATED]')
def runserver(debug, console_log, use_reloader, address, port, timeout, workers, socket):
    """Starts a web server"""
    init_pilot()
    debug = debug or config.get("DEBUG") or console_log
    if debug:
        print(Fore.BLUE + '-=' * 20)
        print(
            Fore.YELLOW + 'Starting Superset server in ' +
            Fore.RED + 'DEBUG' +
            Fore.YELLOW + ' mode')
        print(Fore.BLUE + '-=' * 20)
        print(Style.RESET_ALL)
        if console_log:
            console_log_run(app, port, use_reloader)
        else:
            debug_run(app, port, use_reloader)
    else:
        addr_str = ' unix:{socket} ' if socket else' {address}:{port} '
        cmd = (
            'gunicorn '
            '-w {workers} '
            '--timeout {timeout} '
            '-b ' + addr_str +
            '--limit-request-line 0 '
            '--limit-request-field_size 0 '
            'superset:app').format(**locals())
        print(Fore.GREEN + 'Starting server with command: ')
        print(Fore.YELLOW + cmd)
        print(Style.RESET_ALL)
        Popen(cmd, shell=True).wait()


@manager.option(
    '-v', '--verbose', action='store_true',
    help='Show extra information')
def version(verbose):
    """Prints the current version number"""
    print(Fore.BLUE + '-=' * 15)
    print(Fore.YELLOW + 'Pilot ' + Fore.CYAN + '{version}'.format(
        version=config.get('VERSION_STRING')))
    print(Fore.BLUE + '-=' * 15)
    if verbose:
        print('[DB] : ' + '{}'.format(db.engine))
    print(Style.RESET_ALL)


@manager.option(
    '-t', '--load-test-data', action='store_true',
    help="Load additional test data")
def load_examples(load_test_data, user_id=None):
    """Loads a set of Slices and Dashboards and a supporting dataset """
    logging.info("Loading examples into {}".format(db))

    data.load_css_templates(user_id=user_id)

    logging.info('Loading energy related dataset')
    data.load_energy(user_id=user_id)

    logging.info("Loading [World Bank's Health Nutrition and Population Stats]")
    data.load_world_bank_health_n_pop(user_id=user_id)

    logging.info('Loading [Birth names]')
    data.load_birth_names(user_id=user_id)

    logging.info('Loading [Random time series data]')
    data.load_random_time_series_data(user_id=user_id)

    logging.info('Loading [Random long/lat data]')
    data.load_long_lat_data(user_id=user_id)

    # logging.info('Loading [chinese population]')
    # data.load_chinese_population(user_id=user_id)

    logging.info('Loading [Country Map data]')
    data.load_country_map_data(user_id=user_id)

    logging.info('Loading [Multiformat time series]')
    data.load_multiformat_time_series_data(user_id=user_id)

    logging.info('Loading [Misc Charts] dashboard')
    data.load_misc_dashboard(user_id=user_id)

    logging.info('Loading [Paris GeoJson]')
    data.load_paris_iris_geojson(user_id=user_id)

    logging.info('Loading [San Francisco population polygons]')
    data.load_sf_population_polygons(user_id=user_id)

    logging.info('Loading [Flights data]')
    data.load_flights(user_id=user_id)

    logging.info('Loading [BART lines]')
    data.load_bart_lines(user_id=user_id)

    logging.info('Loading [Multi Line]')
    data.load_multi_line(user_id=user_id)

    if load_test_data:
        logging.info('Loading [Unicode test data]')
        data.load_unicode_test_data(user_id=user_id)

    logging.info('Loading DECK.gl demo')
    data.load_deck_dash(user_id=user_id)


@manager.option(
    '-d', '--datasource',
    help=('Specify which datasource name to load, if omitted, all '
          'datasources will be refreshed'),
)
@manager.option(
    '-m', '--merge',
    action='store_true',
    help="Specify using 'merge' property during operation.",
    default=False,)
def refresh_druid(datasource, merge):
    """Refresh druid datasources"""
    session = db.session()
    from superset.models.druid import DruidCluster
    for cluster in session.query(DruidCluster).all():
        try:
            cluster.refresh_datasources(datasource_name=datasource,
                                        merge_flag=merge)
        except Exception as e:
            print(
                "Error while processing cluster '{}'\n{}".format(
                    cluster, str(e)))
            logging.exception(e)
        cluster.metadata_last_refreshed = datetime.now()
        print(
            'Refreshed metadata from cluster '
            '[' + cluster.cluster_name + ']')
    session.commit()


@manager.option(
    '-p', '--path', dest='path',
    help='Path to a single YAML file or path containing multiple YAML '
         'files to import (*.yaml or *.yml)')
@manager.option(
    '-s', '--sync', dest='sync', default='',
    help='comma seperated list of element types to synchronize '
         'e.g. "metrics,columns" deletes metrics and columns in the DB '
         'that are not specified in the YAML file')
@manager.option(
    '-r', '--recursive', dest='recursive', action='store_true',
    help='recursively search the path for yaml files')
def import_datasources(path, sync, recursive=False):
    """Import datasources from YAML"""
    sync_array = sync.split(',')
    p = Path(path)
    files = []
    if p.is_file():
        files.append(p)
    elif p.exists() and not recursive:
        files.extend(p.glob('*.yaml'))
        files.extend(p.glob('*.yml'))
    elif p.exists() and recursive:
        files.extend(p.rglob('*.yaml'))
        files.extend(p.rglob('*.yml'))
    for f in files:
        logging.info('Importing datasources from file %s', f)
        try:
            with f.open() as data_stream:
                dict_import_export_util.import_from_dict(
                    db.session,
                    yaml.safe_load(data_stream),
                    sync=sync_array)
        except Exception as e:
            logging.error('Error when importing datasources from file %s', f)
            logging.error(e)


@manager.option(
    '-f', '--datasource-file', default=None, dest='datasource_file',
    help='Specify the the file to export to')
@manager.option(
    '-p', '--print', action='store_true', dest='print_stdout',
    help='Print YAML to stdout')
@manager.option(
    '-b', '--back-references', action='store_true', dest='back_references',
    help='Include parent back references')
@manager.option(
    '-d', '--include-defaults', action='store_true', dest='include_defaults',
    help='Include fields containing defaults')
def export_datasources(print_stdout, datasource_file,
                       back_references, include_defaults):
    """Export datasources to YAML"""
    data = dict_import_export_util.export_to_dict(
        session=db.session,
        recursive=True,
        back_references=back_references,
        include_defaults=include_defaults)
    if print_stdout or not datasource_file:
        yaml.safe_dump(data, stdout, default_flow_style=False)
    if datasource_file:
        logging.info('Exporting datasources to %s', datasource_file)
        with open(datasource_file, 'w') as data_stream:
            yaml.safe_dump(data, data_stream, default_flow_style=False)


@manager.option(
    '-b', '--back-references', action='store_false',
    help='Include parent back references')
def export_datasource_schema(back_references):
    """Export datasource YAML schema to stdout"""
    data = dict_import_export_util.export_schema_to_dict(
        back_references=back_references)
    yaml.safe_dump(data, stdout, default_flow_style=False)


@manager.command
def update_datasources_cache():
    """Refresh sqllab datasources cache"""
    from superset.models.connection import Database
    for database in db.session.query(Database).all():
        print('Fetching {} datasources ...'.format(database.name))
        try:
            database.all_table_names(force=True)
            database.all_view_names(force=True)
        except Exception as e:
            print('{}'.format(str(e)))


@manager.option(
    '-w', '--workers',
    type=int,
    help='Number of celery server workers to fire up')
def worker(workers):
    """Starts a Superset worker for async SQL query execution."""
    logging.info(
        "The 'superset worker' command is deprecated. Please use the 'celery "
        "worker' command instead.")
    if workers:
        celery_app.conf.update(CELERYD_CONCURRENCY=workers)
    elif config.get('SUPERSET_CELERY_WORKERS'):
        celery_app.conf.update(
            CELERYD_CONCURRENCY=config.get('SUPERSET_CELERY_WORKERS'))

    worker = celery_app.Worker(optimization='fair')
    worker.start()


@manager.option(
    '-p', '--port',
    default='5555',
    help='Port on which to start the Flower process')
@manager.option(
    '-a', '--address',
    default='localhost',
    help='Address on which to run the service')
def flower(port, address):
    """Runs a Celery Flower web server

    Celery Flower is a UI to monitor the Celery operation on a given
    broker"""
    BROKER_URL = celery_app.conf.BROKER_URL
    cmd = (
        'celery flower '
        '--broker={BROKER_URL} '
        '--port={port} '
        '--address={address} '
    ).format(**locals())
    logging.info(
        "The 'superset flower' command is deprecated. Please use the 'celery "
        "flower' command instead.")
    print(Fore.GREEN + 'Starting a Celery Flower instance')
    print(Fore.BLUE + '-=' * 40)
    print(Fore.YELLOW + cmd)
    print(Fore.BLUE + '-=' * 40)
    Popen(cmd, shell=True).wait()

