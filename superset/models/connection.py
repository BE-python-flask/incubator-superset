# -*- coding: utf-8 -*-
# pylint: disable=C,R,W
"""A collection of ORM sqlalchemy models for Pilot"""
from copy import copy, deepcopy
import os
import json
import textwrap
import logging
import numpy
import sqlparse
import pandas as pd
from flask import g
from flask_appbuilder import Model

import sqlalchemy as sqla
from sqlalchemy.orm import backref, relationship
from sqlalchemy_utils import EncryptedType
from sqlalchemy import (
    Column, Integer, String, ForeignKey, Text, Boolean, Table,
    create_engine, MetaData, select, UniqueConstraint,
)
from sqlalchemy.engine.url import make_url
from sqlalchemy.sql import text
from sqlalchemy.sql.expression import TextAsFrom
from sqlalchemy.orm.session import make_transient
from sqlalchemy.pool import NullPool

from superset import db, app, db_engine_specs, conf, utils, security_manager
from superset.cache import TokenCache
from superset.cas.keytab import download_keytab
from superset.utils import GUARDIAN_AUTH
from superset.exceptions import GuardianException, PropertyException
from superset.message import DISABLE_GUARDIAN_FOR_KEYTAB, DISABLE_CAS
from .base import AuditMixinNullable, ImportMixin

config = app.config

custom_password_store = config.get('SQLALCHEMY_CUSTOM_PASSWORD_STORE')
PASSWORD_MASK = 'X' * 10


class Database(Model, AuditMixinNullable, ImportMixin):
    __tablename__ = 'dbs'
    type = "table"
    model_type = 'database'
    guardian_type = model_type.upper()

    id = Column(Integer, primary_key=True)
    database_name = Column(String(128), nullable=False, unique=True)
    verbose_name = Column(String(250), unique=True)
    description = Column(Text)
    online = Column(Boolean, default=False)
    sqlalchemy_uri = Column(String(1024))
    password = Column(EncryptedType(String(1024), config.get('SECRET_KEY')))
    cache_timeout = Column(Integer)
    select_as_create_table_as = Column(Boolean, default=False)
    expose = Column(Boolean, default=True)
    allow_run_sync = Column(Boolean, default=True)
    allow_run_async = Column(Boolean, default=False)
    allow_ctas = Column(Boolean, default=False)
    allow_dml = Column(Boolean, default=True)
    force_ctas_schema = Column(String(64))
    allow_multi_schema_metadata_fetch = Column(Boolean, default=True)
    args = Column(Text, default=textwrap.dedent("""\
    {
        "connect_args": {}
    }
    """))
    perm = Column(String(1000))
    impersonate_user = Column(Boolean, default=False)

    __table_args__ = (
        UniqueConstraint('database_name', name='database_name_uc'),
    )
    export_fields = ('database_name', 'description', 'online', 'sqlalchemy_uri',
                     'args', 'password', 'expose')

    database_types = ['INCEPTOR', 'MYSQL', 'ORACLE', 'MSSQL']
    addable_types = database_types

    def __repr__(self):
        return self.database_name

    @property
    def name(self):
        return self.database_name

    @property
    def data(self):
        return {
            'name': self.database_name,
            'backend': self.backend,
            'allow_multi_schema_metadata_fetch':
                self.allow_multi_schema_metadata_fetch,
        }

    @property
    def unique_name(self):
        return self.database_name

    @classmethod
    def name_column(cls):
        return cls.database_name

    @property
    def database_type(self):
        return self.backend.upper()

    @property
    def backend(self):
        url = make_url(self.sqlalchemy_uri_decrypted)
        return url.get_backend_name()

    @property
    def is_inceptor(self):
        if self.backend and self.backend.lower() == 'inceptor':
            return True
        else:
            return False

    @property
    def perm(self):
        return "[{obj.database_name}].(id:{obj.id})".format(obj=self)

    @classmethod
    def get_password_masked_url_from_uri(cls, uri):
        url = make_url(uri)
        return cls.get_password_masked_url(url)

    @classmethod
    def get_password_masked_url(cls, url):
        url_copy = deepcopy(url)
        if url_copy.password is not None and url_copy.password != PASSWORD_MASK:
            url_copy.password = PASSWORD_MASK
        return url_copy

    def set_sqlalchemy_uri(self, uri):
        conn = sqla.engine.url.make_url(uri)
        if conn.password != PASSWORD_MASK:
            # do not over-write the password with the password mask
            self.password = conn.password
        conn.password = PASSWORD_MASK if conn.password else None
        self.sqlalchemy_uri = str(conn)  # hides the password

    def get_effective_user(self, url, user_name=None):
        """
        Get the effective user, especially during impersonation.
        :param url: SQL Alchemy URL object
        :param user_name: Default username
        :return: The effective username
        """
        effective_username = None
        if self.impersonate_user:
            effective_username = url.username
            if user_name:
                effective_username = user_name
            elif (
                            hasattr(g, 'user') and hasattr(g.user, 'username') and
                            g.user.username is not None
            ):
                effective_username = g.user.username
        return effective_username

    @utils.memoized(watch=('impersonate_user', 'sqlalchemy_uri_decrypted', 'extra'))
    def get_sqla_engine(self, schema=None, nullpool=True, user_name=None):
        extra = self.get_extra()
        url = make_url(self.sqlalchemy_uri_decrypted)
        url = self.db_engine_spec.adjust_database_uri(url, schema)
        if url.drivername == 'oracle':
            os.environ["NLS_LANG"] = "SIMPLIFIED CHINESE_CHINA.UTF8"
        effective_username = self.get_effective_user(url, user_name)
        # If using MySQL or Presto for example, will set url.username
        # If using Hive, will not do anything yet since that relies on a
        # configuration parameter instead.
        self.db_engine_spec.modify_url_for_impersonation(
            url,
            self.impersonate_user,
            effective_username)

        masked_url = self.get_password_masked_url(url)
        logging.info('Database.get_sqla_engine(). Masked URL: {0}'.format(masked_url))

        params = extra.get('engine_params', {})
        if nullpool:
            params['poolclass'] = NullPool

        # If using Hive, this will set hive.server2.proxy.user=$effective_username
        configuration = {}
        configuration.update(
            self.db_engine_spec.get_configuration_for_impersonation(
                str(url),
                self.impersonate_user,
                effective_username))
        if configuration:
            params['connect_args'] = {'configuration': configuration}

        DB_CONNECTION_MUTATOR = config.get('DB_CONNECTION_MUTATOR')
        if DB_CONNECTION_MUTATOR:
            url, params = DB_CONNECTION_MUTATOR(
                url, params, effective_username, security_manager)
        return create_engine(url, **params)

    def get_reserved_words(self):
        return self.get_dialect().preparer.reserved_words

    def get_quoter(self):
        return self.get_dialect().identifier_preparer.quote

    def get_df(self, sql, schema):
        sqls = [str(s).strip().strip(';') for s in sqlparse.parse(sql)]
        eng = self.get_sqla_engine(schema=schema)
        for i in range(len(sqls) - 1):
            eng.execute(sqls[i])
        df = pd.read_sql_query(sqls[-1], eng)

        def needs_conversion(df_series):
            if df_series.empty:
                return False
            if isinstance(df_series[0], (list, dict)):
                return True
            return False

        for k, v in df.dtypes.items():
            if v.type == numpy.object_ and needs_conversion(df[k]):
                df[k] = df[k].apply(utils.json_dumps_w_dates)
        return df

    def compile_sqla_query(self, qry, schema=None):
        eng = self.get_sqla_engine(schema=schema)
        compiled = qry.compile(eng, compile_kwargs={"literal_binds": True})
        return '{}'.format(compiled)

    def select_star(self, table_name, schema=None, limit=100, show_cols=False,
                   indent=True, latest_partition=True, cols=None):
        """Generates a ``select *`` statement in the proper dialect"""
        eng = self.get_sqla_engine(schema=schema)
        return self.db_engine_spec.select_star(
            self, table_name, schema=schema, engine=eng,
            limit=limit, show_cols=show_cols,
            indent=indent, latest_partition=latest_partition, cols=cols)

    def wrap_sql_limit(self, sql, limit=100):
        qry = (
            select('*')
                .select_from(TextAsFrom(text(sql), ['*'])
                             .alias('inner_qry')).limit(limit)
        )
        return self.compile_sqla_query(qry)

    def apply_limit_to_sql(self, sql, limit=1000):
        return self.db_engine_spec.apply_limit_to_sql(sql, limit, self)

    def safe_sqlalchemy_uri(self):
        return self.sqlalchemy_uri

    @property
    def inspector(self):
        engine = self.get_sqla_engine()
        return sqla.inspect(engine)

    def all_table_names(self, schema=None, force=False):
        if not schema:
            if not self.allow_multi_schema_metadata_fetch:
                return []
            tables_dict = self.db_engine_spec.fetch_result_sets(
                self, 'table', force=force)
            return tables_dict.get('', [])
        return sorted(
            self.db_engine_spec.get_table_names(schema, self.inspector))

    def all_view_names(self, schema=None, force=False):
        if not schema:
            if not self.allow_multi_schema_metadata_fetch:
                return []
            views_dict = self.db_engine_spec.fetch_result_sets(
                self, 'view', force=force)
            return views_dict.get('', [])
        views = []
        try:
            views = self.inspector.get_view_names(schema)
        except Exception:
            pass
        return views

    def all_schema_names(self):
        return sorted(self.db_engine_spec.get_schema_names(self.inspector))

    @property
    def db_engine_spec(self):
        return db_engine_specs.engines.get(
            self.backend, db_engine_specs.BaseEngineSpec)

    def grains(self):
        """Defines time granularity database-specific expressions.

        The idea here is to make it easy for users to change the time grain
        form a datetime (maybe the source grain is arbitrary timestamps, daily
        or 5 minutes increments) to another, "truncated" datetime. Since
        each database has slightly different but similar datetime functions,
        this allows a mapping between database engines and actual functions.
        """
        return self.db_engine_spec.time_grains

    def grains_dict(self):
        """Allowing to lookup grain by either label or duration

        For backward compatibility"""
        d = {grain.duration: grain for grain in self.grains()}
        d.update({grain.label: grain for grain in self.grains()})
        return d

    def get_extra(self):
        return self.get_args()

    def get_args(self):
        args = {}
        if self.args:
            try:
                args = json.loads(self.args)
            except Exception as e:
                logging.error(e)
        return args

    def get_table(self, table_name, schema=None):
        args = self.get_args()
        meta = MetaData(**args.get('metadata_params', {}))
        return Table(
            table_name, meta,
            schema=schema or None,
            autoload=True,
            autoload_with=self.get_sqla_engine())

    def get_columns(self, table_name, schema=None):
        return self.inspector.get_columns(table_name, schema)

    def get_column(self, table_name, column_name, schema=None):
        columns = self.inspector.get_columns(table_name, schema)
        if columns:
            for c in columns:
                if c.get('name', '') == column_name:
                    return c
        return {}

    def get_indexes(self, table_name, schema=None):
        return self.inspector.get_indexes(table_name, schema)

    def get_pk_constraint(self, table_name, schema=None):
        return self.inspector.get_pk_constraint(table_name, schema)

    def get_foreign_keys(self, table_name, schema=None):
        return self.inspector.get_foreign_keys(table_name, schema)

    @property
    def sqlalchemy_uri_decrypted(self):
        conn = sqla.engine.url.make_url(self.sqlalchemy_uri)
        if custom_password_store:
            conn.password = custom_password_store(conn)
        else:
            conn.password = self.password
        return str(conn)

    @property
    def sql_url(self):
        return '/superset/sql/{}/'.format(self.id)

    def get_dialect(self):
        sqla_url = make_url(self.sqlalchemy_uri_decrypted)
        return sqla_url.get_dialect()()

    @classmethod
    def append_args(cls, connect_args):
        def get_keytab(username, password):
            dir = config.get('GLOBAL_FOLDER', '/tmp/pilot')
            if not os.path.exists(dir):
                os.makedirs(dir)
            path = os.path.join(dir, '{}.keytab'.format(username))
            if conf.get(GUARDIAN_AUTH) and not conf.get('CAS_AUTH'):
                from superset.guardian import guardian_client as client
                client.download_keytab(username, password, path)
            elif conf.get('CAS_AUTH'):
                token = TokenCache.get(username, password)
                download_keytab(username, path, token=token)
            else:
                raise GuardianException(DISABLE_GUARDIAN_FOR_KEYTAB)
            return path

        def get_ticket():
            raise NotImplementedError

        mech = connect_args.get('mech', '').lower()
        if mech == 'kerberos':
            connect_args['keytab'] = get_keytab(g.user.username, g.user.password2)
        elif mech == 'token':
            keys = [k.lower() for k in connect_args.keys()]
            if 'guardiantoken' not in keys:
                connect_args['guardianToken'] = \
                    TokenCache.get(g.user.username, g.user.password2)
        elif mech == 'ticket':
            connect_args['casTicket'] = get_ticket()
        return connect_args

    @classmethod
    def count(cls):
        return db.session.query(cls) \
            .filter(cls.database_name != config.get("METADATA_CONN_NAME")) \
            .count()

    @classmethod
    def import_obj(cls, session, i_db, solution, grant_owner_perms):
        make_transient(i_db)
        i_db.id = None
        existed_db = cls.get_object(name=i_db.name)
        new_db = existed_db

        if not existed_db:
            logging.info('Importing database connection: [{}] (add)'.format(i_db))
            new_db = i_db.copy()
            session.add(new_db)
            session.commit()
            grant_owner_perms([cls.guardian_type, new_db.database_name])
        else:
            policy, new_name = cls.get_policy(cls.model_type, i_db.name, solution)
            if policy == cls.Policy.OVERWRITE:
                logging.info('Importing database connection: [{}] (overwrite)'
                             .format(i_db))
                existed_db.override(i_db)
                session.commit()
            elif policy == cls.Policy.RENAME:
                logging.info('Importing database connection: [{}] (rename to [{}])'
                             .format(i_db, new_name))
                new_db = i_db.copy()
                new_db.database_name = new_name
                session.add(new_db)
                session.commit()
                grant_owner_perms([cls.guardian_type, new_db.database_name])
            elif policy == cls.Policy.SKIP:
                logging.info('Importing database connection: [{}] (skip)'.format(i_db))

        return new_db


class HDFSConnection(Model, AuditMixinNullable, ImportMixin):
    __tablename__ = 'hdfs_connection'
    type = 'table'
    model_type = 'hdfsconnection'
    guardian_type = model_type.upper()

    id = Column(Integer, primary_key=True)
    connection_name = Column(String(128), nullable=False, unique=True)
    description = Column(Text)
    online = Column(Boolean, default=False)
    database_id = Column(Integer, ForeignKey('dbs.id'), nullable=True)
    httpfs = Column(String(64), nullable=False)
    database = relationship(
        'Database',
        backref=backref('hdfs_connection', lazy='dynamic'),
        foreign_keys=[database_id])

    __table_args__ = (
        UniqueConstraint('connection_name', name='hdfs_connection_name_uc'),
    )
    export_fields = ('connection_name', 'description', 'online', 'database_id', 'httpfs')

    connection_types = ['HDFS', ]
    addable_types = connection_types

    def __repr__(self):
        return self.connection_name

    @property
    def name(self):
        return self.connection_name

    @classmethod
    def name_column(cls):
        return cls.connection_name

    @classmethod
    def import_obj(cls, session, i_hdfsconn, solution, grant_owner_perms):
        make_transient(i_hdfsconn)
        i_hdfsconn.id = None
        existed_hdfsconn = cls.get_object(name=i_hdfsconn.name)
        new_hdfsconn = existed_hdfsconn

        if not existed_hdfsconn:
            logging.info('Importing hdfs connection: [{}] (add)'.format(i_hdfsconn))
            new_hdfsconn = i_hdfsconn.copy()
            session.add(new_hdfsconn)
            session.commit()
            grant_owner_perms([cls.guardian_type, new_hdfsconn.connection_name])
        else:
            policy, new_name = cls.get_policy(cls.model_type, i_hdfsconn.name, solution)
            if policy == cls.Policy.OVERWRITE:
                logging.info('Importing hdfs connection: [{}] (overwrite)'
                             .format(i_hdfsconn))
                existed_hdfsconn.override(i_hdfsconn)
                session.commit()
            elif policy == cls.Policy.RENAME:
                logging.info('Importing hdfs connection: [{}] (rename to [{}])'
                             .format(i_hdfsconn, new_name))
                new_hdfsconn = i_hdfsconn.copy()
                new_hdfsconn.connection_name = new_name
                session.add(new_hdfsconn)
                session.commit()
                grant_owner_perms([cls.guardian_type, new_hdfsconn.connection_name])
            elif policy == cls.Policy.SKIP:
                logging.info('Importing hdfs connection: [{}] (skip)'.format(i_hdfsconn))

        return new_hdfsconn


class Connection(object):
    connection_types = Database.database_types + HDFSConnection.connection_types

    @classmethod
    def count(cls):
        return Database.count() + HDFSConnection.count()
