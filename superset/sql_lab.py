
from datetime import datetime
import json
import logging
import pandas as pd
from time import sleep
import uuid

from celery.exceptions import SoftTimeLimitExceeded
from contextlib2 import contextmanager
import sqlalchemy
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import NullPool

from superset import app, db, utils, dataframe, results_backend, security_manager
from superset.models import Database, Query
from superset.sql_parse import SupersetQuery
from superset.timeout_decorator import sql_timeout
from superset.utils import get_celery_app, QueryStatus


config = app.config
celery_app = get_celery_app(config)
stats_logger = app.config.get('STATS_LOGGER')
SQLLAB_TIMEOUT = config.get('SQLLAB_ASYNC_TIME_LIMIT_SEC', 600)


INCEPTOR_WORK_SCHEMA = 'pilot'  # The schema used for download sql result as csv
INCEPTOR_TEMP_TABLE_PREFIX = 'pilot_sqllab_'


def dedup(l, suffix='__'):
    """De-duplicates a list of string by suffixing a counter

    Always returns the same number of entries as provided, and always returns
    unique values.

    >>> dedup(['foo', 'bar', 'bar', 'bar'])
    ['foo', 'bar', 'bar__1', 'bar__2']
    """
    new_l = []
    seen = {}
    for s in l:
        if s in seen:
            seen[s] += 1
            s += suffix + str(seen[s])
        else:
            seen[s] = 0
        new_l.append(s)
    return new_l


class SqlLabException(Exception):
    pass


def get_query(query_id, session, retry_count=5):
    """attemps to get the query and retry if it cannot"""
    query = None
    attempt = 0
    while not query and attempt < retry_count:
        try:
            query = session.query(Query).filter_by(id=query_id).one()
        except Exception:
            attempt += 1
            logging.error(
                'Query with id `{}` could not be retrieved'.format(query_id))
            stats_logger.incr('error_attempting_orm_query_' + str(attempt))
            logging.error('Sleeping for a sec before retrying...')
            sleep(1)
    if not query:
        stats_logger.incr('error_failed_at_getting_orm_query')
        raise SqlLabException('Failed at getting query')
    return query


@contextmanager
def session_scope(nullpool):
    """Provide a transactional scope around a series of operations."""
    if nullpool:
        engine = sqlalchemy.create_engine(
            app.config.get('SQLALCHEMY_DATABASE_URI'), poolclass=NullPool)
        session_class = sessionmaker()
        session_class.configure(bind=engine)
        session = session_class()
    else:
        session = db.session()
        session.commit()  # HACK

    try:
        yield session
        session.commit()
    except Exception as e:
        session.rollback()
        logging.exception(e)
        raise
    finally:
        session.close()


@sql_timeout
@celery_app.task(bind=True, soft_time_limit=SQLLAB_TIMEOUT)
def get_sql_results(ctask, query_id, rendered_query, return_results=True,
                    store_results=False, user_name=None):
    """Executes the sql query returns the results."""
    with session_scope(not ctask.request.called_directly) as session:

        try:
            return execute(ctask, query_id, rendered_query, return_results,
                               store_results, user_name, session=session)
        except Exception as e:
            logging.exception(e)
            stats_logger.incr('error_sqllab_unhandled')
            query = get_query(query_id, session)
            query.error_message = str(e)
            query.status = QueryStatus.FAILED
            query.tmp_table_name = None
            session.commit()
            raise


def execute(ctask, query_id, rendered_query, return_results=True,
            store_results=False, user_name=None, session=None):
    """Executes the sql query returns the results."""
    SQL_MAX_ROWS = app.config.get('SQL_MAX_ROW')
    query = get_query(query_id, session)
    payload = dict(query_id=query_id)

    database = query.database
    db_engine_spec = database.db_engine_spec
    db_engine_spec.patch()

    def handle_error(msg):
        """Local method handling error while processing the SQL"""
        troubleshooting_link = config['TROUBLESHOOTING_LINK']
        query.error_message = msg
        query.status = QueryStatus.FAILED
        query.tmp_table_name = None
        session.commit()
        payload.update({
            'status': query.status,
            'error': msg,
        })
        if troubleshooting_link:
            payload['link'] = troubleshooting_link
        return payload

    if store_results and not results_backend:
        return handle_error("Results backend isn't configured.")

    # query.executed_sql = executed_sql
    query.status = QueryStatus.RUNNING
    query.start_running_time = utils.now_as_float()
    session.merge(query)
    session.commit()

    engine = database.get_sqla_engine(
        schema=query.schema,
        nullpool=not ctask.request.called_directly,
        user_name=user_name,
    )
    conn = engine.raw_connection()
    cursor = conn.cursor()
    logging.info('Running query: \n{}'.format(rendered_query))

    # Limit enforced only for retrieving the data, not for the CTA queries.
    sqls = rendered_query.strip().rstrip(';').split(';')
    sqls = [s.strip().strip('\n') for s in sqls]
    for sql in sqls:
        superset_query = SupersetQuery(sql)
        executed_sql = superset_query.stripped()
        # if not superset_query.is_select() and not database.allow_dml:
        #     return handle_error(
        #         'Only `SELECT` statements are allowed against this database')
        # if query.select_as_cta:
        #     if not superset_query.is_select():
        #         return handle_error(
        #             'Only `SELECT` statements can be used with the CREATE TABLE '
        #             'feature.')
        #     if not query.tmp_table_name:
        #         start_dttm = datetime.fromtimestamp(query.start_time)
        #         query.tmp_table_name = 'tmp_{}_table_{}'.format(
        #             query.user_id, start_dttm.strftime('%Y_%m_%d_%H_%M_%S'))
        #     executed_sql = superset_query.as_create_table(query.tmp_table_name)
        #     query.select_as_cta_used = True
        if (superset_query.is_select() and SQL_MAX_ROWS and
                (not query.limit or query.limit > SQL_MAX_ROWS)):
            query.limit = SQL_MAX_ROWS
            executed_sql = database.apply_limit_to_sql(executed_sql, query.limit)

        # Hook to allow environment-specific mutation (usually comments) to the SQL
        # SQL_QUERY_MUTATOR = config.get('SQL_QUERY_MUTATOR')
        # if SQL_QUERY_MUTATOR:
        #     executed_sql = SQL_QUERY_MUTATOR(
        #         executed_sql, user_name, security_manager, database)

        try:
            # logging.info(query.executed_sql)
            cursor.execute(executed_sql, **db_engine_spec.cursor_execute_kwargs)
        except SoftTimeLimitExceeded as e:
            logging.exception(e)
            if conn is not None:
                conn.close()
            return handle_error(
                "SQL Lab timeout. This environment's policy is to kill queries "
                'after {} seconds.'.format(SQLLAB_TIMEOUT))
        except Exception as e:
            logging.exception(e)
            if conn is not None:
                conn.close()
            return handle_error(db_engine_spec.extract_error_message(e))

    db_engine_spec.handle_cursor(cursor, query, session)
    data = db_engine_spec.fetch_data(cursor, query.limit)

    if conn is not None:
        conn.commit()
        conn.close()

    if query.status == utils.QueryStatus.STOPPED:
        return handle_error('The query has been stopped')

    cdf = dataframe.SupersetDataFrame(data, cursor.description, db_engine_spec)

    query.rows = cdf.size
    query.progress = 100
    query.status = QueryStatus.SUCCESS
    # if query.select_as_cta:
    #     query.select_sql = '{}'.format(
    #         database.select_star(
    #             query.tmp_table_name,
    #             limit=query.limit,
    #             schema=database.force_ctas_schema,
    #             show_cols=False,
    #             latest_partition=False))
    query.end_time = utils.now_as_float()
    session.merge(query)
    session.flush()

    payload.update({
        'status': query.status,
        'data': cdf.data if cdf.data else [],
        'columns': cdf.columns if cdf.columns else [],
        'query': query.to_dict(),
    })
    # if store_results:
    #     key = '{}'.format(uuid.uuid4())
    #     logging.info('Storing results in results backend, key: {}'.format(key))
    #     json_payload = json.dumps(payload, default=utils.json_iso_dttm_ser)
    #     cache_timeout = database.cache_timeout
    #     if cache_timeout is None:
    #         cache_timeout = config.get('CACHE_DEFAULT_TIMEOUT', 0)
    #     results_backend.set(key, utils.zlib_compress(json_payload), cache_timeout)
    #     query.results_key = key
    #     query.end_result_backend_time = utils.now_as_float()

    session.merge(query)
    session.commit()

    if return_results:
        return payload


@sql_timeout
def execute_sql(database_id, sql, schema=None):
    database = Database.get_object(database_id)
    engine = database.get_sqla_engine(schema=schema)

    query = SupersetQuery(sql)
    if query.is_select():
        sql = database.wrap_sql_limit(sql, int(app.config.get('SQL_MAX_ROW', 100)))

    result_proxy = engine.execute(sql)
    cdf = None
    column_names = []
    if result_proxy.cursor:
        column_names = [col[0] for col in result_proxy.cursor.description]
        column_names = dedup(column_names)
        data = result_proxy.fetchall()
        cdf = dataframe.SupersetDataFrame(pd.DataFrame(data, columns=column_names))

    payload = {
        'data': cdf.data if cdf else [],
        'columns': column_names,
        'sql': sql
    }
    return payload


def store_sql_results_to_hdfs(select_sql, engine):
    """
    For inceptor, store the sql results to hdfs folders
    :param sql: origin select sql
    :param engine: inceptor engine
    :return: temp table name and hdfs path storing results
    """
    ts = datetime.now().isoformat()
    ts = ts.replace('-', '').replace(':', '').split('.')[0]
    table_name = '{}{}'.format(INCEPTOR_TEMP_TABLE_PREFIX, ts).lower()
    path = '/tmp/pilot/{}/'.format(table_name)
    table_name = '{}.{}'.format(INCEPTOR_WORK_SCHEMA, table_name).lower()

    connect = engine.connect()

    sql = 'CREATE DATABASE IF NOT EXISTS {}'.format(INCEPTOR_WORK_SCHEMA)
    _execute(connect, sql)

    sql = 'DROP TABLE IF EXISTS {}'.format(table_name)
    _execute(connect, sql)

    sql = "CREATE TABLE {table} STORED AS CSVFILE LOCATION '{path}' as {sql}"\
        .format(table=table_name, path=path, sql=select_sql)
    _execute(connect, sql)

    sql = "SET ngmr.partition.automerge=TRUE"
    _execute(connect, sql)
    sql = "SET ngmr.partition.mergesize.mb=180"
    _execute(connect, sql)

    sql = "INSERT OVERWRITE TABLE {table} SELECT * FROM {table}".format(table=table_name)
    _execute(connect, sql)
    return table_name, path


@sql_timeout
def _execute(connect, sql):
    logging.info(sql)
    connect.execute(sql)


def drop_inceptor_temp_table(username):
    """Drop redundant temp tables in inceptor created when downloading sql results.
    """
    keep_temp_table_name = 3

    logging.info('Begin to drop redundant temp tables in Inceptor')
    default_inceptor = db.session.query(Database)\
        .filter_by(database_name=app.config.get('DEFAULT_INCEPTOR_CONN_NAME'))\
        .one()
    engine = default_inceptor.get_sqla_engine()
    sql = "SELECT table_name, create_time FROM system.tables_v " \
          "WHERE database_name='{schema}' and owner_name='{owner}' " \
          "      and table_name like '{prefix}%' " \
          "ORDER BY create_time DESC LIMIT {offset}, 10" \
        .format(schema=INCEPTOR_WORK_SCHEMA,
                owner=username,
                prefix=INCEPTOR_TEMP_TABLE_PREFIX,
                offset=keep_temp_table_name)
    logging.info(sql)

    rs = engine.execute(sql)
    for row in rs:
        sql = 'DROP TABLE IF EXISTS {}.{}'.format(INCEPTOR_WORK_SCHEMA, row[0])
        logging.info(sql)
        engine.execute(sql)
