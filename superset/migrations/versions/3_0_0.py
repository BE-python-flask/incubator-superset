"""merge superset 0.26.0

Revision ID: 3_0_0
Revises: None
Create Date: 2018-7-17
"""
import json
from alembic import op
import sqlalchemy as sa
from sqlalchemy import (Column, Integer, or_, String, Text, Table, ForeignKey)
from sqlalchemy.ext.declarative import declarative_base
from flask_appbuilder.models.mixins import AuditMixin
from sqlalchemy.orm import relationship

from superset import db

# revision identifiers, used by Alembic.
revision = '3_0_0'
down_revision = '2_0_0'


Base = declarative_base()


class User(Base):
    """Declarative class to do query in upgrade"""
    __tablename__ = 'ab_user'
    id = Column(Integer, primary_key=True)

slice_user = Table('slice_user', Base.metadata,
                   Column('id', Integer, primary_key=True),
                   Column('user_id', Integer, ForeignKey('ab_user.id')),
                   Column('slice_id', Integer, ForeignKey('slices.id'))
                   )

dashboard_user = Table(
    'dashboard_user', Base.metadata,
    Column('id', Integer, primary_key=True),
    Column('user_id', Integer, ForeignKey('ab_user.id')),
    Column('dashboard_id', Integer, ForeignKey('dashboards.id'))
)


class Slice(Base):
    __tablename__ = 'slices'
    id = Column(Integer, primary_key=True)
    viz_type = Column(String(250))
    params = Column(Text)
    owners = relationship("User", secondary=slice_user)


class Dashboard(Base, AuditMixin):
    """Declarative class to do query in upgrade"""
    __tablename__ = 'dashboards'
    id = Column(Integer, primary_key=True)
    owners = relationship("User", secondary=dashboard_user)


def upgrade():
    op.create_table('access_request',
        sa.Column('created_on', sa.DateTime(), nullable=True),
        sa.Column('changed_on', sa.DateTime(), nullable=True),
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('datasource_type', sa.String(length=200), nullable=True),
        sa.Column('datasource_id', sa.Integer(), nullable=True),
        sa.Column('changed_by_fk', sa.Integer(), nullable=True),
        sa.Column('created_by_fk', sa.Integer(), nullable=True),
        sa.ForeignKeyConstraint(['changed_by_fk'], ['ab_user.id'], ),
        sa.ForeignKeyConstraint(['created_by_fk'], ['ab_user.id'], ),
        sa.PrimaryKeyConstraint('id')
    )
    op.create_table('annotation_layer',
        sa.Column('created_on', sa.DateTime(), nullable=True),
        sa.Column('changed_on', sa.DateTime(), nullable=True),
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('name', sa.String(length=250), nullable=True),
        sa.Column('descr', sa.Text(), nullable=True),
        sa.Column('changed_by_fk', sa.Integer(), nullable=True),
        sa.Column('created_by_fk', sa.Integer(), nullable=True),
        sa.ForeignKeyConstraint(['changed_by_fk'], ['ab_user.id'], ),
        sa.ForeignKeyConstraint(['created_by_fk'], ['ab_user.id'], ),
        sa.PrimaryKeyConstraint('id')
    )
    op.create_table('annotation',
        sa.Column('created_on', sa.DateTime(), nullable=True),
        sa.Column('changed_on', sa.DateTime(), nullable=True),
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('start_dttm', sa.DateTime(), nullable=True),
        sa.Column('end_dttm', sa.DateTime(), nullable=True),
        sa.Column('layer_id', sa.Integer(), nullable=True),
        sa.Column('short_descr', sa.String(length=500), nullable=True),
        sa.Column('long_descr', sa.Text(), nullable=True),
        sa.Column('changed_by_fk', sa.Integer(), nullable=True),
        sa.Column('created_by_fk', sa.Integer(), nullable=True),
        sa.ForeignKeyConstraint(['changed_by_fk'], ['ab_user.id'], ),
        sa.ForeignKeyConstraint(['created_by_fk'], ['ab_user.id'], ),
        sa.ForeignKeyConstraint(['layer_id'], ['annotation_layer.id'], ),
        sa.PrimaryKeyConstraint('id')
    )
    # op.create_index('ti_dag_state',
    #     'annotation', ['layer_id', 'start_dttm', 'end_dttm'], unique=False)
    op.create_table('clusters',
        sa.Column('created_on', sa.DateTime(), nullable=False),
        sa.Column('changed_on', sa.DateTime(), nullable=False),
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('cluster_name', sa.String(length=250), nullable=True),
        sa.Column('coordinator_host', sa.String(length=255), nullable=True),
        sa.Column('coordinator_port', sa.Integer(), nullable=True),
        sa.Column('coordinator_endpoint', sa.String(length=255), nullable=True),
        sa.Column('broker_host', sa.String(length=255), nullable=True),
        sa.Column('broker_port', sa.Integer(), nullable=True),
        sa.Column('broker_endpoint', sa.String(length=255), nullable=True),
        sa.Column('metadata_last_refreshed', sa.DateTime(), nullable=True),
        sa.Column('cache_timeout', sa.Integer(), nullable=True),
        sa.Column('verbose_name', sa.String(length=250), nullable=True),
        sa.Column('created_by_fk', sa.Integer(), sa.ForeignKey("ab_user.id"), nullable=True),
        sa.Column('changed_by_fk', sa.Integer(), sa.ForeignKey("ab_user.id"), nullable=True),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('cluster_name')
        )
    op.create_table('css_templates',
        sa.Column('created_on', sa.DateTime(), nullable=False),
        sa.Column('changed_on', sa.DateTime(), nullable=False),
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('template_name', sa.String(length=250), nullable=True),
        sa.Column('css', sa.Text(), nullable=True),
        sa.Column('changed_by_fk', sa.Integer(), nullable=True),
        sa.Column('created_by_fk', sa.Integer(), nullable=True),
        sa.ForeignKeyConstraint(['changed_by_fk'], ['ab_user.id'], ),
        sa.ForeignKeyConstraint(['created_by_fk'], ['ab_user.id'], ),
        sa.PrimaryKeyConstraint('id')
        )
    op.create_table('saved_query',
        sa.Column('created_on', sa.DateTime(), nullable=True),
        sa.Column('changed_on', sa.DateTime(), nullable=True),
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('user_id', sa.Integer(), nullable=True),
        sa.Column('db_id', sa.Integer(), nullable=True),
        sa.Column('label', sa.String(256), nullable=True),
        sa.Column('schema', sa.String(128), nullable=True),
        sa.Column('sql', sa.Text(), nullable=True),
        sa.Column('description', sa.Text(), nullable=True),
        sa.Column('changed_by_fk', sa.Integer(), nullable=True),
        sa.Column('created_by_fk', sa.Integer(), nullable=True),
        sa.ForeignKeyConstraint(['changed_by_fk'], ['ab_user.id'], ),
        sa.ForeignKeyConstraint(['created_by_fk'], ['ab_user.id'], ),
        sa.ForeignKeyConstraint(['user_id'], ['ab_user.id'], ),
        sa.ForeignKeyConstraint(['db_id'], ['dbs.id'], ),
        sa.PrimaryKeyConstraint('id')
    )
    op.create_table('url',
        sa.Column('created_on', sa.DateTime(), nullable=False),
        sa.Column('changed_on', sa.DateTime(), nullable=False),
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('url', sa.Text(), nullable=True),
        sa.Column('created_by_fk', sa.Integer(), nullable=True),
        sa.Column('changed_by_fk', sa.Integer(), nullable=True),
        sa.ForeignKeyConstraint(['changed_by_fk'], ['ab_user.id'], ),
        sa.ForeignKeyConstraint(['created_by_fk'], ['ab_user.id'], ),
        sa.PrimaryKeyConstraint('id')
        )
    op.create_table('keyvalue',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('value', sa.Text(), nullable=False),
        sa.PrimaryKeyConstraint('id')
        )
    ############################################
    op.create_table('datasources',
        sa.Column('created_on', sa.DateTime(), nullable=False),
        sa.Column('changed_on', sa.DateTime(), nullable=False),
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('datasource_name', sa.String(length=255), nullable=True),
        sa.Column('is_featured', sa.Boolean(), nullable=True),
        sa.Column('is_hidden', sa.Boolean(), nullable=True),
        sa.Column('description', sa.Text(), nullable=True),
        sa.Column('default_endpoint', sa.Text(), nullable=True),
        sa.Column('user_id', sa.Integer(), sa.ForeignKey("ab_user.id"), nullable=True),
        sa.Column('fetch_values_from', sa.String(length=100), nullable=True),
        sa.Column('perm', sa.String(length=1000), nullable=True),
        sa.Column('offset', sa.String(length=100), nullable=True),
        sa.Column('cache_timeout', sa.String(length=100), nullable=True),
        sa.Column('filter_select_enabled', sa.Boolean(), nullable=True),
        sa.Column('params', sa.String(length=1000), nullable=True),
        sa.Column('cluster_name', sa.String(length=250), sa.ForeignKey("clusters.cluster_name"), nullable=True),
        sa.Column('created_by_fk', sa.Integer(), sa.ForeignKey("ab_user.id"), nullable=True),
        sa.Column('changed_by_fk', sa.Integer(), sa.ForeignKey("ab_user.id"), nullable=True),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('datasource_name')
        )
    op.create_table('columns',
        sa.Column('created_on', sa.DateTime(), nullable=False),
        sa.Column('changed_on', sa.DateTime(), nullable=False),
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('column_name', sa.String(length=255), nullable=True),
        sa.Column('is_active', sa.Boolean(), nullable=True),
        sa.Column('type', sa.String(length=32), nullable=True),
        sa.Column('groupby', sa.Boolean(), nullable=True),
        sa.Column('count_distinct', sa.Boolean(), nullable=True),
        sa.Column('sum', sa.Boolean(), nullable=True),
        sa.Column('max', sa.Boolean(), nullable=True),
        sa.Column('min', sa.Boolean(), nullable=True),
        sa.Column('avg', sa.Boolean(), nullable=True),
        sa.Column('filterable', sa.Boolean(), nullable=True),
        sa.Column('description', sa.Text(), nullable=True),
        sa.Column('dimension_spec_json', sa.Text(), nullable=True),
        sa.Column('verbose_name', sa.String(length=1024), nullable=True),
        sa.Column('datasource_id', sa.Integer(), sa.ForeignKey("datasources.id"), nullable=True),
        sa.Column('created_by_fk', sa.Integer(), sa.ForeignKey("ab_user.id"), nullable=True),
        sa.Column('changed_by_fk', sa.Integer(), sa.ForeignKey("ab_user.id"), nullable=True),
        sa.PrimaryKeyConstraint('id')
        )
    op.create_table('metrics',
        sa.Column('created_on', sa.DateTime(), nullable=False),
        sa.Column('changed_on', sa.DateTime(), nullable=False),
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('metric_name', sa.String(length=512), nullable=True),
        sa.Column('verbose_name', sa.String(length=1024), nullable=True),
        sa.Column('metric_type', sa.String(length=32), nullable=True),
        sa.Column('json', sa.Text(), nullable=True),
        sa.Column('description', sa.Text(), nullable=True),
        sa.Column('is_restricted', sa.Boolean(), nullable=True),
        sa.Column('d3format', sa.String(length=128), nullable=True),
        sa.Column('warning_text', sa.Text(), nullable=True),
        sa.Column('datasource_id', sa.Integer(), sa.ForeignKey("datasources.id"), nullable=True),
        sa.Column('created_by_fk', sa.Integer(), sa.ForeignKey("ab_user.id"), nullable=True),
        sa.Column('changed_by_fk', sa.Integer(), sa.ForeignKey("ab_user.id"), nullable=True),
        sa.PrimaryKeyConstraint('id')
        )
    op.create_table('dashboard_user',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('dashboard_id', sa.Integer(), nullable=False),
        sa.Column('user_id', sa.Integer(), nullable=False),
        sa.ForeignKeyConstraint(['dashboard_id'], ['dashboards.id'], ),
        sa.ForeignKeyConstraint(['user_id'], ['ab_user.id'], ),
        sa.PrimaryKeyConstraint('id')
        )
    op.create_table('slice_user',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('slice_id', sa.Integer(), nullable=False),
        sa.Column('user_id', sa.Integer(), nullable=False),
        sa.ForeignKeyConstraint(['slice_id'], ['slices.id'], ),
        sa.ForeignKeyConstraint(['user_id'], ['ab_user.id'], ),
        sa.PrimaryKeyConstraint('id')
        )

    ######################################### add columns
    op.add_column('dataset', sa.Column('fetch_values_predicate', sa.String(length=1000), nullable=True))
    op.add_column('dataset', sa.Column('template_params', sa.Text(), nullable=True))
    op.add_column('dataset', sa.Column('perm', sa.String(length=1000), nullable=True))
    op.add_column('dataset', sa.Column('is_sqllab_view', sa.Boolean(), nullable=True))

    op.add_column('dbs', sa.Column('perm', sa.String(length=1000), nullable=True))
    op.add_column('dbs', sa.Column('verbose_name', sa.String(length=250), nullable=True))
    op.add_column('dbs', sa.Column('impersonate_user', sa.Boolean(), nullable=True))
    op.add_column('dbs', sa.Column('allow_multi_schema_metadata_fetch', sa.Boolean(), nullable=True))

    op.add_column('query', sa.Column('start_running_time', sa.Numeric(precision=20, scale=6), nullable=True))
    op.add_column('query', sa.Column('end_result_backend_time', sa.Numeric(precision=20, scale=6), nullable=True))
    op.add_column('query', sa.Column('tracking_url', sa.Text(), nullable=True))
    op.drop_column('query', 'limit_reached')

    op.add_column('slices', sa.Column('perm', sa.String(length=2000), nullable=True))
    op.add_column('sql_metrics', sa.Column('warning_text', sa.Text(), nullable=True))

    ######################################### annotation
    bind = op.get_bind()
    session = db.Session(bind=bind)
    for slc in session.query(Slice).filter(
            or_(
                Slice.viz_type.like('line'),
                Slice.viz_type.like('bar'))
    ):
        params = json.loads(slc.params)
        layers = params.get('annotation_layers', [])
        if layers:
            new_layers = []
            for layer in layers:
                new_layers.append({
                    'annotationType': 'INTERVAL',
                    'style': 'solid',
                    'name': 'Layer {}'.format(layer),
                    'show': True,
                    'overrides': {'since': None, 'until': None},
                    'value': layer,
                    'width': 1,
                    'sourceType': 'NATIVE',
                })
            params['annotation_layers'] = new_layers
            slc.params = json.dumps(params)
            session.merge(slc)
            session.commit()
    session.close()

    ######################################### add owner
    objects = session.query(Slice).all()
    objects += session.query(Dashboard).all()
    for obj in objects:
        if obj.created_by and obj.created_by not in obj.owners:
            obj.owners.append(obj.created_by)
        session.commit()
    session.close()


def downgrade():
    ######################################### annotation
    bind = op.get_bind()
    session = db.Session(bind=bind)
    for slc in session.query(Slice).filter(
            or_(
                Slice.viz_type.like('line'),
                Slice.viz_type.like('bar'))
    ):
        params = json.loads(slc.params)
        layers = params.get('annotation_layers', [])
        if layers:
            params['annotation_layers'] = [layer['value'] for layer in layers]
            slc.params = json.dumps(params)
            session.merge(slc)
            session.commit()
    session.close()
    ########################################
    op.drop_column('sql_metrics', 'warning_text')
    op.drop_column('slices', 'perm')
    op.add_column('query', sa.Column('limit_reached', sa.Boolean(), nullable=True))
    op.drop_column('query', 'tracking_url')
    op.drop_column('query', 'end_result_backend_time')
    op.drop_column('query', 'start_running_time')

    op.drop_column('dbs', 'allow_multi_schema_metadata_fetch')
    op.drop_column('dbs', 'impersonate_user')
    op.drop_column('dbs', 'verbose_name')
    op.drop_column('dbs', 'perm')
    op.drop_column('dataset', 'perm')
    op.drop_column('dataset', 'is_sqllab_view')
    op.drop_column('dataset', 'fetch_values_predicate')
    op.drop_column('dataset', 'template_params')

    op.drop_table('slice_user')
    op.drop_table('dashboard_user')
    op.drop_table('metrics')
    op.drop_table('columns')
    op.drop_table('datasources')
    op.drop_table('keyvalue')
    op.drop_table('url')
    op.drop_table('saved_query')
    op.drop_table('css_templates')
    op.drop_table('clusters')
    #op.drop_index('ti_dag_state', 'annotation')
    op.drop_table('annotation')
    op.drop_table('annotation_layer')
    op.drop_table('access_request')
