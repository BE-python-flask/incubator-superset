# -*- coding: utf-8 -*-
# pylint: disable=C,R,W
"""A collection of ORM sqlalchemy models for Pilot"""
import re
import random
import string
import logging
import sqlparse
from io import StringIO
import numpy
import pandas as pd
from datetime import datetime
from distutils.util import strtobool

from flask import g, Markup, escape
from flask_babel import lazy_gettext as _
from flask_appbuilder import Model
import six
import sqlalchemy as sqla
from sqlalchemy import (
    Column, Integer, String, ForeignKey, Text, Boolean,
    DateTime, desc, asc, select, and_, or_, UniqueConstraint
)
from sqlalchemy.orm import backref, relationship
from sqlalchemy.orm.session import make_transient
from sqlalchemy.sql import table, literal_column, text, column
from sqlalchemy.sql.expression import TextAsFrom

from superset import db, app, utils, security_manager
from superset.utils import DTTM_ALIAS
from superset.exceptions import (
    PropertyException, DatabaseException, PermissionException, HDFSException
)
from superset.jinja_context import get_template_processor
from .annotations import Annotation
from .base import AuditMixinNullable, ImportMixin, QueryResult, QueryStatus
from .connection import Database, HDFSConnection
from .datasource_base import BaseColumn, BaseMetric, BaseDatasource

config = app.config


FillterPattern = re.compile(r'''((?:[^,"']|"[^"]*"|'[^']*')+)''')


class AnnotationDatasource(BaseDatasource):
    """ Dummy object so we can query annotations using 'Viz' objects just like
        regular datasources.
    """

    cache_timeout = 0

    def query(self, query_obj):
        df = None
        error_message = None
        qry = db.session.query(Annotation)
        qry = qry.filter(Annotation.layer_id == query_obj['filter'][0]['val'])
        qry = qry.filter(Annotation.start_dttm >= query_obj['from_dttm'])
        qry = qry.filter(Annotation.end_dttm <= query_obj['to_dttm'])
        status = QueryStatus.SUCCESS
        try:
            df = pd.read_sql_query(qry.statement, db.engine)
        except Exception as e:
            status = QueryStatus.FAILED
            logging.exception(e)
            error_message = (
                utils.error_msg_from_exception(e))
        return QueryResult(
            status=status,
            df=df,
            duration=0,
            query='',
            error_message=error_message)

    def get_query_str(self, query_obj):
        raise NotImplementedError()

    def values_for_column(self, column_name, limit=10000):
        raise NotImplementedError()


class TableColumn(Model, BaseColumn):

    """ORM object for table columns, each table can have multiple columns"""

    __tablename__ = 'table_columns'
    model_type = 'column'

    dataset_id = Column(Integer, ForeignKey('dataset.id'))
    ref_dataset = relationship(
        'Dataset',
        backref=backref('ref_columns', cascade='all, delete-orphan'),
        foreign_keys=[dataset_id])
    is_dttm = Column(Boolean, default=False)
    expression = Column(Text, default='')
    python_date_format = Column(String(256))
    database_expression = Column(String(256))

    __table_args__ = (
        UniqueConstraint('column_name', 'dataset_id', name='column_name_dataset_uc'),
    )

    export_fields = (
        'dataset_id', 'column_name', 'verbose_name', 'is_dttm', 'is_active',
        'type', 'groupby', 'count_distinct', 'sum', 'avg', 'max', 'min',
        'filterable', 'expression', 'description', 'python_date_format',
        'database_expression'
    )
    export_parent = 'dataset'
    temp_dataset = None

    def __repr__(self):
        return self.column_name

    @property
    def name(self):
        return self.column_name

    @property
    def dataset(self):
        return self.temp_dataset if self.temp_dataset else self.ref_dataset

    @property
    def sqla_col(self):
        name = self.column_name
        if not self.expression:
            col = column(self.column_name).label(name)
        else:
            col = literal_column(self.expression).label(name)
        return col

    @property
    def table(self):
        return self.ref_dataset

    @property
    def datasource(self):
        return self.table

    def get_time_filter(self, start_dttm, end_dttm):
        col = self.sqla_col.label('__time')
        l = []  # noqa: E741
        if start_dttm:
            l.append(col >= text(self.dttm_sql_literal(start_dttm)))
        if end_dttm:
            l.append(col <= text(self.dttm_sql_literal(end_dttm)))
        return and_(*l)

    def get_timestamp_expression(self, time_grain):
        """Getting the time component of the query"""
        pdf = self.python_date_format
        is_epoch = pdf in ('epoch_s', 'epoch_ms')
        if not self.expression and not time_grain and not is_epoch:
            return column(self.column_name, type_=DateTime).label(DTTM_ALIAS)

        expr = self.expression or self.column_name
        if is_epoch:
            # if epoch, translate to DATE using db specific conf
            db_spec = self.table.database.db_engine_spec
            if pdf == 'epoch_s':
                expr = db_spec.epoch_to_dttm().format(col=expr)
            elif pdf == 'epoch_ms':
                expr = db_spec.epoch_ms_to_dttm().format(col=expr)
        if time_grain:
            grain = self.table.database.grains_dict().get(time_grain)
            if grain:
                expr = grain.function.format(col=expr)
        return literal_column(expr, type_=DateTime).label(DTTM_ALIAS)

    def dttm_sql_literal(self, dttm):
        """Convert datetime object to a SQL expression string

        If database_expression is empty, the internal dttm
        will be parsed as the string with the pattern that
        the user inputted (python_date_format)
        If database_expression is not empty, the internal dttm
        will be parsed as the sql sentence for the database to convert
        """
        tf = self.python_date_format
        if self.database_expression:
            return self.database_expression.format(dttm.strftime('%Y-%m-%d %H:%M:%S'))
        elif tf:
            if tf == 'epoch_s':
                return str((dttm - datetime(1970, 1, 1)).total_seconds())
            elif tf == 'epoch_ms':
                return str((dttm - datetime(1970, 1, 1)).total_seconds() * 1000.0)
            return "'{}'".format(dttm.strftime(tf))
        else:
            s = self.table.database.db_engine_spec.convert_dttm(
                self.type or '', dttm)
            return s or "'{}'".format(dttm.strftime('%Y-%m-%d %H:%M:%S.%f'))

    def get_metrics(self):
        metrics = []
        M = SqlMetric  # noqa
        quoted = self.column_name
        if self.sum:
            metrics.append(M(
                metric_name='sum__' + self.column_name,
                metric_type='sum',
                expression='SUM({})'.format(quoted),
            ))
        if self.avg:
            metrics.append(M(
                metric_name='avg__' + self.column_name,
                metric_type='avg',
                expression='AVG({})'.format(quoted),
            ))
        if self.max:
            metrics.append(M(
                metric_name='max__' + self.column_name,
                metric_type='max',
                expression='MAX({})'.format(quoted),
            ))
        if self.min:
            metrics.append(M(
                metric_name='min__' + self.column_name,
                metric_type='min',
                expression='MIN({})'.format(quoted),
            ))
        if self.count_distinct:
            metrics.append(M(
                metric_name='count_distinct__' + self.column_name,
                metric_type='count_distinct',
                expression='COUNT(DISTINCT {})'.format(quoted),
            ))
        return {m.metric_name: m for m in metrics}


class SqlMetric(Model, BaseMetric):

    """ORM object for metrics, each table can have multiple metrics"""

    __tablename__ = 'sql_metrics'
    model_type = 'metric'

    dataset_id = Column(Integer, ForeignKey('dataset.id'))
    ref_dataset = relationship(
        'Dataset',
        backref=backref('ref_metrics', cascade='all, delete-orphan'),
        foreign_keys=[dataset_id])
    expression = Column(Text)

    __table_args__ = (
        UniqueConstraint('metric_name', 'dataset_id', name='metric_name_dataset_uc'),
    )

    export_fields = (
        'metric_name', 'verbose_name', 'metric_type', 'dataset_id', 'expression',
        'description', 'is_restricted', 'd3format')
    export_parent = 'dataset'
    temp_dataset = None

    def __repr__(self):
        return self.metric_name

    @property
    def name(self):
        return self.metric_name

    @property
    def dataset(self):
        return self.temp_dataset if self.temp_dataset else self.ref_dataset

    @property
    def sqla_col(self):
        name = self.metric_name
        return literal_column(self.expression).label(name)

    @property
    def perm(self):
        return (
            "{parent_name}.[{obj.metric_name}](id:{obj.id})"
        ).format(obj=self,
                 parent_name=self.dataset.full_name) if self.dataset else None


class Dataset(Model, BaseDatasource):
    """An ORM object for SqlAlchemy table references"""
    type = "table"
    __tablename__ = 'dataset'
    model_type = 'dataset'
    query_language = 'sql'
    guardian_type = model_type.upper()

    dataset_name = Column(String(128), nullable=False, unique=True)
    table_name = Column(String(128))
    schema = Column(String(128))
    sql = Column(Text)
    database_id = Column(Integer, ForeignKey('dbs.id'), nullable=True)
    fetch_values_predicate = Column(String(1000))
    user_id = Column(Integer, ForeignKey('ab_user.id'))
    database = relationship(
        'Database',
        backref=backref('dataset'),
        foreign_keys=[database_id])
    owner = relationship(
        'User',
        backref='dataset',
        foreign_keys=[user_id])

    online = Column(Boolean, default=False)
    main_dttm_col = Column(String(128))
    is_sqllab_view = Column(Boolean, default=False)
    template_params = Column(Text)

    __table_args__ = (
        UniqueConstraint('dataset_name', name='dataset_name_uc'),
    )

    baselink = "table"
    column_cls = TableColumn
    metric_cls = SqlMetric
    temp_columns = []      # for creating slice with source table
    temp_metrics = []
    export_fields = (
        'table_name', 'main_dttm_col', 'description', 'default_endpoint',
        'database_id', 'offset', 'cache_timeout', 'schema',
        'sql', 'params', 'template_params')
    export_parent = 'database'
    export_children = ['metrics', 'columns']

    dataset_types = Database.database_types
    filter_types = dataset_types
    addable_types = ['DATABASE']

    sqla_aggregations = {
        'COUNT_DISTINCT': lambda column_name: sqla.func.COUNT(sqla.distinct(column_name)),
        'COUNT': sqla.func.COUNT,
        'SUM': sqla.func.SUM,
        'AVG': sqla.func.AVG,
        'MIN': sqla.func.MIN,
        'MAX': sqla.func.MAX,
    }

    def __repr__(self):
        return self.dataset_name

    @property
    def name(self):
        return self.dataset_name

    @classmethod
    def name_column(cls):
        return cls.dataset_name

    @property
    def dataset_type(self):
        if self.hdfs_table:
            return self.hdfs_table.hdfs_table_type
        elif self.database:
            return self.database.database_type
        else:
            return None

    @property
    def backend(self):
        if self.database:
            return self.database.backend
        else:
            return None

    @property
    def connection(self):
        if self.hdfs_table:
            return self.hdfs_table.hdfs_path
        elif self.database:
            return str(self.database)
        else:
            return None

    @property
    def columns(self):
        return self.temp_columns if self.temp_columns else self.ref_columns

    @property
    def metrics(self):
        return self.temp_metrics if self.temp_metrics else self.ref_metrics

    @property
    def description_markeddown(self):
        return utils.markdown(self.description)

    @property
    def link(self):
        name = escape(self.name)
        return Markup(
            '<a href="{self.explore_url}">{name}</a>'.format(**locals()))

    @property
    def schema_perm(self):
        """Returns schema permission if present, database one otherwise."""
        return security_manager.get_schema_perm(self.database, self.schema)

    def get_perm(self):
        return "[{obj.database}].[{obj.dataset_name}](id:{obj.id})".format(obj=self)

    @property
    def table(self):
        if not self.schema:
            return self.table_name
        return "{}.{}".format(self.schema, self.table_name)

    @property
    def full_name(self):
        # return utils.get_datasource_full_name(
        #     self.database, self.table_name, schema=self.schema)
        user = self.created_by.username if self.created_by else None
        return "[{}].[{}].[{}]".format(user, self.database, self.dataset_name)

    @property
    def dttm_cols(self):
        l = [c.column_name for c in self.columns if c.is_dttm]
        if self.main_dttm_col not in l:
            l.append(self.main_dttm_col)
        return l

    @property
    def num_cols(self):
        return [c.column_name for c in self.columns if c.isnum]

    @property
    def any_dttm_col(self):
        cols = self.dttm_cols
        if cols:
            return cols[0]

    @property
    def html(self):
        t = ((c.column_name, c.type) for c in self.columns)
        df = pd.DataFrame(t)
        df.columns = ['field', 'type']
        return df.to_html(
            index=False,
            classes=(
                "dataframe table table-striped table-bordered "
                "table-condensed"))

    @property
    def sql_url(self):
        return self.database.sql_url + "?table_name=" + str(self.table_name)

    @property
    def time_column_grains(self):
        return {
            "time_columns": self.dttm_cols,
            "time_grains": [grain.name for grain in self.database.grains()]
        }

    def get_col(self, col_name):
        columns = self.columns
        for col in columns:
            if col_name == col.column_name:
                return col

    @property
    def data(self):
        d = super(Dataset, self).data
        if self.type == 'table':
            grains = self.database.grains() or []
            if grains:
                grains = [(g.duration, g.name) for g in grains]
            d['granularity_sqla'] = utils.choicify(self.dttm_cols)
            d['time_grain_sqla'] = grains
        return d

    def values_for_column(self, column_name, limit=500):
        """Runs query against sqla to retrieve some
        sample values for the given column.
        """
        cols = {col.column_name: col for col in self.columns}
        target_col = cols[column_name]
        tp = self.get_template_processor()
        db_engine_spec = self.database.db_engine_spec

        qry = (
            select([target_col.sqla_col])
                .select_from(self.get_from_clause(tp, db_engine_spec))
                .distinct()
        )
        if limit:
            qry = qry.limit(limit)

        if self.fetch_values_predicate:
            tp = self.get_template_processor()
            qry = qry.where(tp.process_template(self.fetch_values_predicate))

        engine = self.database.get_sqla_engine()
        sql = '{}'.format(
            qry.compile(engine, compile_kwargs={'literal_binds': True}),
        )

        df = pd.read_sql_query(sql=sql, con=engine)
        return [row[0] for row in df.to_records(index=False)]

    def get_template_processor(self, **kwargs):
        return get_template_processor(
            table=self, database=self.database, **kwargs)

    def get_query_str(self, query_obj):
        engine = self.database.get_sqla_engine()
        qry = self.get_sqla_query(**query_obj)
        sql = six.text_type(
            qry.compile(
                engine,
                compile_kwargs={'literal_binds': True},
            ),
        )
        logging.info(sql)
        sql = sqlparse.format(sql, reindent=True)
        if query_obj['is_prequery']:
            query_obj['prequeries'].append(sql)
        return sql

    def get_sqla_table(self):
        tbl = table(self.table_name)
        if self.schema:
            tbl.schema = self.schema
        return tbl

    def get_from_clause(self, template_processor=None, db_engine_spec=None):
        # Supporting arbitrary SQL statements in place of tables
        if self.sql:
            from_sql = self.sql
            if template_processor:
                from_sql = template_processor.process_template(from_sql)
            from_sql = sqlparse.format(from_sql, strip_comments=True)
            return TextAsFrom(sqla.text(from_sql), []).alias('expr_qry')
        return self.get_sqla_table()

    def adhoc_metric_to_sa(self, metric, cols):
        """
        Turn an adhoc metric into a sqlalchemy column.

        :param dict metric: Adhoc metric definition
        :param dict cols: Columns for the current table
        :returns: The metric defined as a sqlalchemy column
        :rtype: sqlalchemy.sql.column
        """
        expressionType = metric.get('expressionType')
        if expressionType == utils.ADHOC_METRIC_EXPRESSION_TYPES['SIMPLE']:
            column_name = metric.get('column').get('column_name')
            sa_column = column(column_name)
            table_column = cols.get(column_name)

            if table_column:
                sa_column = table_column.sqla_col

            sa_metric = self.sqla_aggregations[metric.get('aggregate')](sa_column)
            sa_metric = sa_metric.label(metric.get('label'))
            return sa_metric
        elif expressionType == utils.ADHOC_METRIC_EXPRESSION_TYPES['SQL']:
            sa_metric = literal_column(metric.get('sqlExpression'))
            sa_metric = sa_metric.label(metric.get('label'))
            return sa_metric
        else:
            return None

    def get_sqla_query(  # sqla
            self,
            groupby, metrics,
            granularity,
            from_dttm, to_dttm,
            filter=None,  # noqa
            is_timeseries=True,
            timeseries_limit=15,
            timeseries_limit_metric=None,
            row_limit=None,
            inner_from_dttm=None,
            inner_to_dttm=None,
            orderby=None,
            extras=None,
            columns=None,
            order_desc=True,
            prequeries=None,
            is_prequery=False,
    ):
        """Querying any sqla table from this common interface"""
        template_kwargs = {
            'from_dttm': from_dttm,
            'groupby': groupby,
            'metrics': metrics,
            'row_limit': row_limit,
            'to_dttm': to_dttm,
            'filter': filter,
            'columns': {col.column_name: col for col in self.columns},
        }
        template_kwargs.update(self.template_params_dict)
        template_processor = self.get_template_processor(**template_kwargs)
        db_engine_spec = self.database.db_engine_spec

        orderby = orderby or []

        # For backward compatibility
        if granularity not in self.dttm_cols:
            granularity = self.main_dttm_col

        # Database spec supports join-free timeslot grouping
        time_groupby_inline = db_engine_spec.time_groupby_inline

        cols = {col.column_name: col for col in self.columns}
        metrics_dict = {m.metric_name: m for m in self.metrics}

        if not granularity and is_timeseries:
            raise Exception(_(
                'Datetime column not provided as part table configuration '
                'and is required by this type of chart'))
        if not groupby and not metrics and not columns:
            raise Exception(_('Empty query?'))
        metrics_exprs = []
        for m in metrics:
            if utils.is_adhoc_metric(m):
                metrics_exprs.append(self.adhoc_metric_to_sa(m, cols))
            elif m in metrics_dict:
                metrics_exprs.append(metrics_dict.get(m).sqla_col)
            else:
                raise Exception(_("Metric '{}' is not valid".format(m)))
        if metrics_exprs:
            main_metric_expr = metrics_exprs[0]
        else:
            main_metric_expr = literal_column('COUNT(*)').label('ccount')

        select_exprs = []
        groupby_exprs = []

        if groupby:
            select_exprs = []
            inner_select_exprs = []
            inner_groupby_exprs = []
            for s in groupby:
                col = cols[s]
                outer = col.sqla_col
                inner = col.sqla_col.label(col.column_name + '__')

                groupby_exprs.append(outer)
                select_exprs.append(outer)
                inner_groupby_exprs.append(inner)
                inner_select_exprs.append(inner)
        elif columns:
            for s in columns:
                select_exprs.append(cols[s].sqla_col)
            metrics_exprs = []

        if granularity:
            dttm_col = cols[granularity]
            time_grain = extras.get('time_grain_sqla')
            time_filters = []

            if is_timeseries:
                timestamp = dttm_col.get_timestamp_expression(time_grain)
                select_exprs += [timestamp]
                groupby_exprs += [timestamp]

            # Use main dttm column to support index with secondary dttm columns
            if db_engine_spec.time_secondary_columns and \
                            self.main_dttm_col in self.dttm_cols and \
                            self.main_dttm_col != dttm_col.column_name:
                time_filters.append(cols[self.main_dttm_col].
                                    get_time_filter(from_dttm, to_dttm))
            time_filters.append(dttm_col.get_time_filter(from_dttm, to_dttm))

        select_exprs += metrics_exprs
        qry = sqla.select(select_exprs)

        tbl = self.get_from_clause(template_processor, db_engine_spec)

        if not columns:
            qry = qry.group_by(*groupby_exprs)

        where_clause_and = []
        having_clause_and = []
        for flt in filter:
            if not all([flt.get(s) for s in ['col', 'op']]):
                continue
            col = flt['col']
            op = flt['op']
            col_obj = cols.get(col)
            if col_obj:
                is_list_target = op in ('in', 'not in')
                eq = self.filter_values_handler(
                    flt.get('val'),
                    target_column_is_numeric=col_obj.is_num,
                    is_list_target=is_list_target)
                if op in ('in', 'not in'):
                    cond = col_obj.sqla_col.in_(eq)
                    if '<NULL>' in eq:
                        cond = or_(cond, col_obj.sqla_col == None)  # noqa
                    if op == 'not in':
                        cond = ~cond
                    where_clause_and.append(cond)
                else:
                    if col_obj.is_num:
                        eq = utils.string_to_num(flt['val'])
                    if op == '==':
                        where_clause_and.append(col_obj.sqla_col == eq)
                    elif op == '!=':
                        where_clause_and.append(col_obj.sqla_col != eq)
                    elif op == '>':
                        where_clause_and.append(col_obj.sqla_col > eq)
                    elif op == '<':
                        where_clause_and.append(col_obj.sqla_col < eq)
                    elif op == '>=':
                        where_clause_and.append(col_obj.sqla_col >= eq)
                    elif op == '<=':
                        where_clause_and.append(col_obj.sqla_col <= eq)
                    elif op == 'LIKE':
                        where_clause_and.append(col_obj.sqla_col.like(eq))
                    elif op == 'IS NULL':
                        where_clause_and.append(col_obj.sqla_col == None)  # noqa
                    elif op == 'IS NOT NULL':
                        where_clause_and.append(col_obj.sqla_col != None)  # noqa
        if extras:
            where = extras.get('where')
            if where:
                where = template_processor.process_template(where)
                where_clause_and += [sqla.text('({})'.format(where))]
            having = extras.get('having')
            if having:
                having = template_processor.process_template(having)
                having_clause_and += [sqla.text('({})'.format(having))]
        if granularity:
            qry = qry.where(and_(*(time_filters + where_clause_and)))
        else:
            qry = qry.where(and_(*where_clause_and))
        qry = qry.having(and_(*having_clause_and))

        if not orderby and not columns:
            orderby = [(main_metric_expr, not order_desc)]

        for col, ascending in orderby:
            direction = asc if ascending else desc
            if utils.is_adhoc_metric(col):
                col = self.adhoc_metric_to_sa(col, cols)
            qry = qry.order_by(direction(col))

        if row_limit:
            qry = qry.limit(row_limit)

        if is_timeseries and \
                timeseries_limit and groupby and not time_groupby_inline:
            if self.database.db_engine_spec.inner_joins:
                # some sql dialects require for order by expressions
                # to also be in the select clause -- others, e.g. vertica,
                # require a unique inner alias
                inner_main_metric_expr = main_metric_expr.label('mme_inner__')
                inner_select_exprs += [inner_main_metric_expr]
                subq = select(inner_select_exprs)
                subq = subq.select_from(tbl)
                inner_time_filter = dttm_col.get_time_filter(
                    inner_from_dttm or from_dttm,
                    inner_to_dttm or to_dttm,
                    )
                subq = subq.where(and_(*(where_clause_and + [inner_time_filter])))
                subq = subq.group_by(*inner_groupby_exprs)

                ob = inner_main_metric_expr
                if timeseries_limit_metric:
                    if utils.is_adhoc_metric(timeseries_limit_metric):
                        ob = self.adhoc_metric_to_sa(timeseries_limit_metric, cols)
                    elif timeseries_limit_metric in metrics_dict:
                        timeseries_limit_metric = metrics_dict.get(
                            timeseries_limit_metric,
                        )
                        ob = timeseries_limit_metric.sqla_col
                    else:
                        raise Exception(_("Metric '{}' is not valid".format(m)))
                direction = desc if order_desc else asc
                subq = subq.order_by(direction(ob))
                subq = subq.limit(timeseries_limit)

                on_clause = []
                for i, gb in enumerate(groupby):
                    on_clause.append(
                        groupby_exprs[i] == column(gb + '__'))

                tbl = tbl.join(subq.alias(), and_(*on_clause))
            else:
                # run subquery to get top groups
                subquery_obj = {
                    'prequeries': prequeries,
                    'is_prequery': True,
                    'is_timeseries': False,
                    'row_limit': timeseries_limit,
                    'groupby': groupby,
                    'metrics': metrics,
                    'granularity': granularity,
                    'from_dttm': inner_from_dttm or from_dttm,
                    'to_dttm': inner_to_dttm or to_dttm,
                    'filter': filter,
                    'orderby': orderby,
                    'extras': extras,
                    'columns': columns,
                    'order_desc': True,
                }
                result = self.query(subquery_obj)
                dimensions = [c for c in result.df.columns if c not in metrics]
                top_groups = self._get_top_groups(result.df, dimensions)
                qry = qry.where(top_groups)

        return qry.select_from(tbl)

    def _get_top_groups(self, df, dimensions):
        cols = {col.column_name: col for col in self.columns}
        groups = []
        for unused, row in df.iterrows():
            group = []
            for dimension in dimensions:
                col_obj = cols.get(dimension)
                group.append(col_obj.sqla_col == row[dimension])
            groups.append(and_(*group))

        return or_(*groups)

    def query(self, query_obj):
        qry_start_dttm = datetime.now()
        sql = self.get_query_str(query_obj)
        status = QueryStatus.SUCCESS
        error_message = None
        df = None
        try:
            df = self.database.get_df(sql, self.schema)
        except Exception as e:
            status = QueryStatus.FAILED
            logging.exception(e)
            error_message = (
                self.database.db_engine_spec.extract_error_message(e))

        # if this is a main query with prequeries, combine them together
        if not query_obj['is_prequery']:
            query_obj['prequeries'].append(sql)
            sql = ';\n\n'.join(query_obj['prequeries'])
        sql += ';'

        return QueryResult(
            status=status,
            df=df,
            duration=datetime.now() - qry_start_dttm,
            query=sql,
            error_message=error_message)

    def preview_data(self, limit=100):
        tbl = table(self.table_name)
        if self.schema:
            tbl.schema = self.schema
        if self.sql:
            tbl = TextAsFrom(sqla.text(self.sql), []).alias('expr_qry')
        qry = select("*").select_from(tbl).limit(limit)
        engine = self.database.get_sqla_engine()
        sql = str(qry.compile(engine, compile_kwargs={"literal_binds": True},))

        df = pd.read_sql(sql, con=engine)
        df = df.replace({numpy.nan: 'None'})
        columns = list(df.columns)
        types = []
        if self.table_name:
            tb = self.get_sqla_table_object()
            col_types = {col.name: str(col.type) for col in tb.columns}
            types = [col_types.get(c) for c in columns]
        return {'columns': columns,
                'types': types,
                'records': df.to_dict(orient='records')}

    def drop_temp_view(self, engine, view_name):
        drop_view = "DROP VIEW {}".format(view_name)
        engine.execute(drop_view)

    def create_temp_view(self, engine, view_name, sql):
        create_view = "CREATE VIEW {} AS {}".format(view_name, sql)
        engine.execute(create_view)

    def get_sqla_table_object(self):
        if not self.database:
            err = 'Missing connection for dataset: [{}]'.format(self.dataset_name)
            logging.error(err)
            raise PropertyException(err)

        try:
            engine = self.database.get_sqla_engine()
            if self.sql:
                view_name = "pilot_view_{}" \
                    .format(''.join(random.sample(string.ascii_lowercase, 10)))
                self.create_temp_view(engine, view_name, self.sql)
                table = self.database.get_table(view_name)
                self.drop_temp_view(engine, view_name)
                return table
            else:
                return self.database.get_table(self.table_name, schema=self.schema)
        except sqla.exc.DBAPIError as e:
            err = _("Drop or create temporary view by sql failed: {msg}") \
                .format(msg=str(e))
            logging.error(err)
            raise DatabaseException(err)
        except Exception as e:
            raise DatabaseException(_(
                "Couldn't fetch table [{table}]'s information in the specified "
                "database [{schema}]")
                                    .format(table=self.table_name, schema=self.schema) + ": " + str(e))

    @classmethod
    def temp_dataset(cls, database_id, full_tb_name, need_columns=True):
        """A temp dataset for slice"""
        dataset = cls(online=True,
                      filter_select_enabled=True)
        if '.' in full_tb_name:
            dataset.schema, dataset.table_name = full_tb_name.split('.')
        else:
            dataset.table_name = full_tb_name
        dataset.dataset_name = '{}_{}'.format(
            dataset.table_name,
            ''.join(random.sample(string.ascii_lowercase, 10))
        )
        dataset.database_id = database_id
        dataset.database = db.session.query(Database) \
            .filter_by(id=database_id).first()
        if need_columns:
            dataset.set_temp_columns_and_metrics()
        return dataset

    def set_temp_columns_and_metrics(self):
        """Get table's columns and metrics"""
        self.temp_columns, self.temp_metrics = self.generate_columns_and_metrics()
        for column in self.temp_columns:
            column.temp_dataset = self
        for metric in self.temp_columns:
            metric.temp_dataset = self
        if self.temp_columns and self.temp_columns[0].is_dttm:
            self.main_dttm_col = self.temp_columns[0].name

    def fetch_metadata(self):
        """Fetches the metadata for the table and merges it in.
        TableColumn.column_name and SqlMetric.metric_name are not case sensitive,
        so need to compare with xxx_name.lower().
        """
        old_column_names = [c.name.lower() for c in self.ref_columns]
        old_metric_names = [m.name.lower() for m in self.ref_metrics]

        new_columns, new_metrics = self.generate_columns_and_metrics()
        for c in new_columns:
            if c.name.lower() not in old_column_names:
                c.dataset_id = self.id
                self.ref_columns.append(c)
        for m in new_metrics:
            if m.name.lower() not in old_metric_names:
                m.dataset_id = self.id
                self.ref_metrics.append(m)

        if not self.main_dttm_col and new_columns and new_columns[0].is_dttm:
            self.main_dttm_col = new_columns[0].name
        db.session.merge(self)
        db.session.commit()

    def generate_columns_and_metrics(self):
        """
        :return: TableColumns[] with date columns at front and SqlMetrics[]
        """
        sqla_table = self.get_sqla_table_object()
        db_dialect = self.database.get_dialect()
        columns, metrics = [], []
        for col in sqla_table.columns:
            try:
                datatype = col.type.compile(dialect=db_dialect).upper()
                # For MSSQL the data type may be
                # NVARCHAR(128) COLLATE "SQL_LATIN1_GENERAL_CP1_CI_AS"
                datatype = datatype.split(' ')[0] if ') ' in datatype else datatype
            except Exception as e:
                datatype = "UNKNOWN"
                logging.error("Unrecognized data type in {}.{}".format(table, col.name))
                logging.exception(e)

            dbcol = TableColumn(column_name=col.name,
                                type=datatype,
                                expression=col.name)
            dbcol.count_distinct = dbcol.is_int or dbcol.is_bool or dbcol.is_string \
                                   or dbcol.is_time
            dbcol.groupby = dbcol.is_int or dbcol.is_bool or dbcol.is_string \
                            or dbcol.is_time
            dbcol.filterable = True
            dbcol.sum = dbcol.is_num
            dbcol.avg = dbcol.is_num
            dbcol.max = dbcol.is_num
            dbcol.min = dbcol.is_num
            dbcol.is_dttm = dbcol.is_time
            if dbcol.is_dttm:
                columns.insert(0, dbcol)
            else:
                columns.append(dbcol)

            metrics += dbcol.get_metrics().values()

        metrics.append(SqlMetric(
            metric_name='count(*)',
            metric_type='count',
            expression="COUNT(*)"
        ))
        return columns, metrics

    @classmethod
    def check_online(cls, dataset, raise_if_false=True):
        def check(obj, user_id):
            user_id = int(user_id)
            if (hasattr(obj, 'online') and obj.online is True) or \
                            obj.created_by_fk == user_id:
                return True
            return False

        user_id = g.user.get_id()
        if check(dataset, user_id) is False:
            if raise_if_false:
                raise PermissionException(_(
                    "Dependent someone's dataset [{dataset}] is offline, so it's "
                    "unavailable").format(dataset=dataset))
            else:
                return False
        # database
        if dataset.database and check(dataset.database, user_id) is False:
            if raise_if_false:
                raise PermissionException(_(
                    "Dependent someone's database connection [{conn}] is offline,  "
                    "so it's unavailable").format(conn=dataset.database))
            else:
                return False
        # hdfs_connection
        if dataset.hdfs_table \
                and dataset.hdfs_table.hdfs_connection \
                and check(dataset.hdfs_table.hdfs_connection, user_id) is False:
            if raise_if_false:
                raise PermissionException(_(
                    "Dependent someone's HDFS connection [{conn}] is offline, so it's "
                    "unavailable").format(conn=dataset.hdfs_table.hdfs_connection))
            else:
                return False
        return True

    @classmethod
    def import_obj(cls, session, i_dataset, solution, grant_owner_perms):
        """Imports the dataset from the object to the database.
        """
        def add_dataset(session, i_dataset, new_dataset, database, hdfsconn):
            if database:
                new_dataset.database_id = database.id
                new_dataset.database = database
            session.add(new_dataset)
            session.commit()
            if i_dataset.hdfs_table:
                i_hdfs_table = i_dataset.hdfs_table
                new_hdfs_table = i_hdfs_table.copy()
                make_transient(new_hdfs_table)
                new_hdfs_table.id = None
                new_hdfs_table.dataset_id = new_dataset.id
                if hdfsconn:
                    new_hdfs_table.hdfs_connection_id = hdfsconn.id
                session.add(new_hdfs_table)
                session.commit()
            return new_dataset

        def overwrite_dataset(session, i_dataset, existed_dataset, database, hdfsconn):
            existed_dataset.override(i_dataset)
            if database:
                existed_dataset.database_id = database.id
                existed_dataset.database = database
            if existed_dataset.hdfs_table:
                existed_hdfs_table = existed_dataset.hdfs_table
                existed_hdfs_table.override(i_dataset.hdfs_table)
                existed_hdfs_table.dataset_id = existed_dataset.id
                existed_hdfs_table.dataset = existed_dataset
                if hdfsconn:
                    existed_hdfs_table.hdfs_connection_id = hdfsconn.id
                    existed_hdfs_table.hdfs_connection = hdfsconn
            session.commit()
            return existed_dataset

        def overwrite_columns_metrics(session, dataset, columns, metrics):
            for c in dataset.ref_columns:
                session.delete(c)
            session.commit()
            for c in columns:
                new_c = c.copy()
                new_c.dataset_id = dataset.id
                session.add(new_c)
                dataset.ref_columns.append(new_c)
            session.commit()

            for m in dataset.ref_metrics:
                session.delete(m)
            session.commit()
            for m in metrics:
                new_m = m.copy()
                new_m.dataset_id = dataset.id
                session.add(new_m)
                dataset.ref_metrics.append(new_m)
            session.commit()

        # Import dependencies
        new_database, new_hdfsconn = None, None
        if i_dataset.database:
            database = i_dataset.database
            new_database = Database.import_obj(
                session, database, solution, grant_owner_perms)
        if i_dataset.hdfs_table and i_dataset.hdfs_table.hdfs_connection:
            hdfsconn = i_dataset.hdfs_table.hdfs_connection
            new_hdfsconn = HDFSConnection.import_obj(
                session, hdfsconn, solution, grant_owner_perms)

        # Import dataset
        make_transient(i_dataset)
        i_dataset.id = None
        existed_dataset = cls.get_object(name=i_dataset.name)
        new_dataset = existed_dataset

        if not existed_dataset:
            logging.info('Importing dataset: [{}] (add)'.format(i_dataset))
            new_dataset = i_dataset.copy()
            new_dataset = add_dataset(
                session, i_dataset, new_dataset, new_database, new_hdfsconn)
            overwrite_columns_metrics(
                session, new_dataset, i_dataset.ref_columns, i_dataset.ref_metrics)
            grant_owner_perms([cls.guardian_type, new_dataset.dataset_name])
        else:
            policy, new_name = cls.get_policy(cls.model_type, i_dataset.name, solution)
            if policy == cls.Policy.OVERWRITE:
                logging.info('Importing dataset: [{}] (overwrite)'.format(i_dataset))
                new_dataset = overwrite_dataset(
                    session, i_dataset, new_dataset, new_database, new_hdfsconn)
                overwrite_columns_metrics(
                    session, new_dataset, i_dataset.ref_columns, i_dataset.ref_metrics)
            elif policy == cls.Policy.RENAME:
                logging.info('Importing dataset: [{}] (rename to [{}])'
                             .format(i_dataset, new_name))
                new_dataset = i_dataset.copy()
                new_dataset.dataset_name = new_name
                new_dataset = add_dataset(
                    session, i_dataset, new_dataset, new_database, new_hdfsconn)
                overwrite_columns_metrics(
                    session, new_dataset, i_dataset.ref_columns, i_dataset.ref_metrics)
                grant_owner_perms([cls.guardian_type, new_dataset.dataset_name])
            elif policy == cls.Policy.SKIP:
                logging.info('Importing dataset: [{}] (skip)'.format(i_dataset))
        return new_dataset

    @classmethod
    def query_datasources_by_name(
            cls, session, database, datasource_name, schema=None):
        query = (
            session.query(cls)
                .filter_by(database_id=database.id)
                .filter_by(table_name=datasource_name)
        )
        if schema:
            query = query.filter_by(schema=schema)
        return query.all()

    @staticmethod
    def default_query(qry):
        return qry.filter_by(is_sqllab_view=False)


class HDFSTable(Model, AuditMixinNullable, ImportMixin):
    __tablename__ = "hdfs_table"
    type = 'table'
    hdfs_table_type = 'HDFS'
    hdfs_table_types = ['HDFS', ]
    filter_types = hdfs_table_types
    addable_types = hdfs_table_types + ['UPLOAD FILE']

    id = Column(Integer, primary_key=True)
    hdfs_path = Column(String(256), nullable=False)
    file_type = Column(String(32))
    separator = Column(String(8), nullable=False, default=',')
    quote = Column(String(8), default='"')
    skip_rows = Column(Integer, default=0)         # skip rows, start with 0
    next_as_header = Column(Boolean, default=False)  # if next line as header
    skip_more_rows = Column(Integer)    # below the header, skip rows again
    nrows = Column(Integer)             # the rows of data readed
    charset = Column(String(32))
    hdfs_connection_id = Column(Integer, ForeignKey('hdfs_connection.id'))
    hdfs_connection = relationship(
        'HDFSConnection',
        backref=backref('hdfs_table'),
        foreign_keys=[hdfs_connection_id]
    )
    dataset_id = Column(Integer, ForeignKey('dataset.id'))
    dataset = relationship(
        'Dataset',
        backref=backref('hdfs_table', uselist=False, cascade='all, delete-orphan'),
        foreign_keys=[dataset_id]
    )

    export_fields = ('hdfs_path', 'file_type', 'separator', 'quote', 'skip_rows',
                     'next_as_header', 'skip_more_rows', 'nrows', 'charset',
                     'hdfs_connection_id', 'dataset_id')

    cache = {}

    def __repr__(self):
        return self.hdfs_path

    @staticmethod
    def create_external_table(database, table_name, columns, hdfs_path,
                              separator=',', schema='default'):
        table_name = '{}.{}'.format(schema, table_name)
        sql = 'create external table {}('.format(table_name)
        names = columns.get('names')
        types = columns.get('types')
        for index, v in enumerate(names):
            sql = sql + names[index] + " " + types[index] + ","
        sql = sql[:-1] \
              + ") row format delimited fields terminated by '" + separator \
              + "' location '" + hdfs_path + "'"

        engine = database.get_sqla_engine()
        engine.execute("drop table if exists " + table_name)
        engine.execute(sql)
        logging.info(sql)

    @classmethod
    def parse_file(cls, file_content, separator=',', quote='"', next_as_header='false',
                   charset='utf-8', nrows='100', names=None):
        header = 0 if strtobool(next_as_header) else None
        nrows = int(nrows)
        try:
            return pd.read_csv(StringIO(file_content), sep=separator, header=header,
                               nrows=nrows, prefix='C', encoding=charset, names=names,
                               quotechar=quote, skip_blank_lines=True)
        except Exception as e:
            cls.cache.clear()
            raise HDFSException(_("Parse file error: {msg}").format(msg=str(e)))

