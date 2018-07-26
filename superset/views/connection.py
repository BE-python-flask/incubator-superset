import json
import requests
import time
from datetime import datetime
from flask import g, request
from flask_babel import lazy_gettext as _
from flask_appbuilder import expose
from flask_appbuilder.models.sqla.interface import SQLAInterface
from flask_appbuilder.security.sqla.models import User

import sqlalchemy as sqla
from sqlalchemy import select, literal, cast, or_, and_
from sqlalchemy.engine.url import make_url

from superset import app, db, models
from superset.timeout_decorator import connection_timeout
from superset.models import Database, HDFSConnection, Connection, Slice
from superset.exception import ParameterException, PermissionException
from superset.views.hdfs import HDFSBrowser, catch_hdfs_exception
from superset.message import *
from .base import (
    SupersetModelView, BaseSupersetView, PageMixin, catch_exception, json_response,
    PermissionManagement
)

config = app.config


class DatabaseView(SupersetModelView, PermissionManagement):  # noqa
    model = models.Database
    model_type = model.model_type
    datamodel = SQLAInterface(models.Database)
    route_base = '/database'
    list_columns = ['id', 'database_name', 'description', 'backend', 'changed_on']
    show_columns = ['id', 'database_name', 'description', 'sqlalchemy_uri',
                    'args', 'backend',  'created_on', 'changed_on']
    add_columns = ['database_name', 'description', 'sqlalchemy_uri', 'args']
    edit_columns = add_columns
    add_template = "superset/models/database/add.html"
    edit_template = "superset/models/database/edit.html"
    base_order = ('changed_on', 'desc')
    description_columns = {}

    str_to_column = {
        'title': Database.database_name,
        'time': Database.changed_on,
        'changed_on': Database.changed_on,
        'owner': User.username
    }

    int_columns = ['id']
    bool_columns = ['expose', 'allow_run_sync', 'allow_dml']
    str_columns = ['created_on', 'changed_on']

    list_template = "superset/databaseList.html"

    def pre_add(self, obj):
        self.check_column_values(obj)
        obj.set_sqlalchemy_uri(obj.sqlalchemy_uri)

    def pre_update(self, old_obj, new_obj):
        if old_obj.database_name == config.get('DEFAULT_INCEPTOR_CONN_NAME'):
            raise PermissionException(CANNOT_EDIT_DEFAULT_CONN)
        super(DatabaseView, self).pre_update(old_obj, new_obj)

    def check_column_values(self, obj):
        if not obj.database_name:
            raise ParameterException(NONE_CONNECTION_NAME)
        self.model.check_name(obj.database_name)
        if not obj.sqlalchemy_uri:
            raise ParameterException(NONE_SQLALCHEMY_URI)
        if not obj.args:
            raise ParameterException(NONE_CONNECTION_ARGS)

    def get_list_args(self, args):
        kwargs = super(DatabaseView, self).get_list_args(args)
        kwargs['database_type'] = args.get('database_type')
        return kwargs

    def get_object_list_data(self, **kwargs):
        """Return the database(connection) list"""
        order_column = kwargs.get('order_column')
        order_direction = kwargs.get('order_direction')
        page = kwargs.get('page')
        page_size = kwargs.get('page_size')
        filter = kwargs.get('filter')
        database_type = kwargs.get('database_type')

        query = db.session.query(Database, User) \
                .outerjoin(User, Database.created_by_fk == User.id)
        query = query.filter(Database.database_name != config.get('METADATA_CONN_NAME'))

        if database_type:
            match_str = '{}%'.format(database_type)
            query = query.filter(
                Database.sqlalchemy_uri.ilike(match_str)
            )
        if filter:
            filter_str = '%{}%'.format(filter.lower())
            query = query.filter(
                or_(
                    Database.database_name.ilike(filter_str),
                    User.username.ilike(filter_str)
                )
            )
        if order_column:
            try:
                column = self.str_to_column.get(order_column)
            except KeyError:
                msg = _('Error order column name: [{name}]').format(name=order_column)
                raise ParameterException(msg)
            else:
                if order_direction == 'desc':
                    query = query.order_by(column.desc())
                else:
                    query = query.order_by(column)

        global_read = True
        readable_names = None
        count = 0
        if self.guardian_auth:
            from superset.guardian import guardian_client as client
            if not client.check_global_read(g.user.username):
                global_read = False
                readable_names = client.search_model_perms(
                    g.user.username, self.model.guardian_type)
                count = len(readable_names)

        if global_read:
            count = query.count()
            if page is not None and page >= 0 and page_size and page_size > 0:
                query = query.limit(page_size).offset(page * page_size)

        rs = query.all()
        data = []
        index = 0
        for obj, user in rs:
            if not global_read:
                if obj.name in readable_names:
                    index += 1
                    if index <= page * page_size:
                        continue
                    elif index > (page+1) * page_size:
                        break
                else:
                    continue
            line = {}
            for col in self.list_columns:
                if col in self.str_columns:
                    line[col] = str(getattr(obj, col, None))
                else:
                    line[col] = getattr(obj, col, None)
            line['created_by_user'] = obj.created_by.username \
                if obj.created_by else None
            data.append(line)

        response = {}
        response['count'] = count
        response['order_column'] = order_column
        response['order_direction'] = 'desc' if order_direction == 'desc' else 'asc'
        response['page'] = page
        response['page_size'] = page_size
        response['data'] = data
        return response

    @catch_exception
    @expose("/online_info/<id>/", methods=['GET'])
    def online_info(self, id):  # Deprecated
        database = self.get_object(id)
        self.check_release_perm(database.guardian_datasource())
        objects = self.release_affect_objects(database)
        info = _("Releasing connection {conn} will make these usable "
                 "for other users: \nDataset: {dataset}, \nSlice: {slice}")\
            .format(conn=objects.get('database'),
                    dataset=objects.get('dataset'),
                    slice=objects.get('slice'))
        return json_response(data=info)

    @catch_exception
    @expose("/offline_info/<id>/", methods=['GET'])
    def offline_info(self, id):  # Deprecated
        database = self.get_object(id)
        self.check_release_perm(database.guardian_datasource())
        objects = self.release_affect_objects(database)
        info = _("Changing connection {conn} to offline will make these "
                 "unusable for other users: \nDataset: {dataset}, \nSlice: {slice}")\
            .format(conn=objects.get('database'),
                    dataset=objects.get('dataset'),
                    slice=objects.get('slice'))
        return json_response(data=info)

    def release_affect_objects(self, database):  # Deprecated
        """
        Changing database to online/offline will affect online_datasets based on this
        and online_slices based on these online_datasets
        """
        online_datasets = [d for d in database.dataset if d.online is True]

        online_dataset_ids = [dataset.id for dataset in online_datasets]
        online_slices = db.session.query(Slice) \
            .filter(
                or_(Slice.datasource_id.in_(online_dataset_ids),
                    Slice.database_id == id),
                Slice.online == 1
        ).all()
        return {'database': [database, ],
                'dataset': online_datasets,
                'slice': online_slices}

    @catch_exception
    @expose("/delete_info/<id>/", methods=['GET'])
    def delete_info(self, id):
        database = self.get_object(id)
        self.check_delete_perm(database.guardian_datasource())
        objects = self.delete_affect_objects(database)
        info = _("Deleting connection {conn} will make these unusable: "
                 "\nDataset: {dataset}, \nSlice: {slice}")\
            .format(conn=objects.get('database'),
                    dataset=objects.get('dataset'),
                    slice=objects.get('slice'))
        return json_response(data=info)

    def delete_affect_objects(self, database):
        """
        Deleting database will affect myself datasets and online datasets.
        myself slices and online slices based on these online_datasets
        """
        user_id = g.user.id
        online_datasets = [d for d in database.dataset if d.online is True]
        myself_datasets = [d for d in database.dataset if d.created_by_fk == user_id]
        online_dataset_ids = [dataset.id for dataset in online_datasets]
        myself_dataset_ids = [dataset.id for dataset in myself_datasets]

        slices = db.session.query(Slice) \
            .filter(
                or_(
                    and_(
                        or_(Slice.datasource_id.in_(online_dataset_ids),
                            Slice.database_id == id),
                        Slice.online == 1
                    ),
                    and_(
                        or_(Slice.datasource_id.in_(myself_dataset_ids),
                            Slice.database_id == id),
                        Slice.created_by_fk == user_id
                    )
                )
        ).all()
        return {'database': [database, ],
                'dataset': list(set(online_datasets + myself_datasets)),
                'slice': slices}

    @catch_exception
    @expose("/grant_info/<id>/", methods=['GET'])
    def grant_info(self, id):
        database = self.get_object(id)
        self.check_grant_perm(database.guardian_datasource())
        return json_response(data="")


class HDFSConnectionModelView(SupersetModelView, PermissionManagement):
    model = models.HDFSConnection
    model_type = model.model_type
    datamodel = SQLAInterface(models.HDFSConnection)
    route_base = '/hdfsconnection'
    list_columns = ['id', 'connection_name']
    show_columns = ['id', 'connection_name', 'description', 'httpfs']
    add_columns = ['connection_name', 'description', 'httpfs']
    edit_columns = add_columns

    str_columns = ['database', ]

    def get_object_list_data(self, **kwargs):
        """Return the hdfs connections.
        There won't be a lot of hdfs conenctions, so just use 'page_size'
        """
        page_size = kwargs.get('page_size')
        query = db.session.query(HDFSConnection) \
            .order_by(HDFSConnection.connection_name.desc())

        global_read = True
        readable_names = None
        count = 0
        if self.guardian_auth:
            from superset.guardian import guardian_client as client
            if not client.check_global_read(g.user.username):
                global_read = False
                readable_names = client.search_model_perms(
                    g.user.username, self.model.guardian_type)
                count = len(readable_names)

        if global_read:
            count = query.count()
            if page_size and page_size > 0:
                query = query.limit(page_size)

        rs = query.all()
        data = []
        index = 0
        for obj in rs:
            if not global_read:
                if obj.name in readable_names:
                    index += 1
                    if index > page_size:
                        break
                else:
                    continue
            line = {}
            for col in self.list_columns:
                if col in self.str_columns:
                    line[col] = str(getattr(obj, col, None))
                else:
                    line[col] = getattr(obj, col, None)
            data.append(line)

        response = {}
        response['count'] = count
        response['page_size'] = page_size
        response['data'] = data
        return response

    def pre_update(self, old_obj, new_obj):
        if old_obj.connection_name == config.get('DEFAULT_HDFS_CONN_NAME'):
            raise PermissionException(CANNOT_EDIT_DEFAULT_CONN)
        super(HDFSConnectionModelView, self).pre_update(old_obj, new_obj)

    def check_column_values(self, obj):
        if not obj.connection_name:
            raise ParameterException(NONE_CONNECTION_NAME)
        self.model.check_name(obj.connection_name)
        if not obj.httpfs:
            raise ParameterException(NONE_HTTPFS)
        if not obj.database_id:
            obj.database_id = None

    @catch_exception
    @expose("/online_info/<id>/", methods=['GET'])
    def online_info(self, id):  # Deprecated
        hdfs_conn = self.get_object(id)
        self.check_release_perm(hdfs_conn.guardian_datasource())
        objects = self.release_affect_objects(hdfs_conn)
        info = _("Releasing connection {conn} will make these usable "
                 "for other users: \nDataset: {dataset}, \nSlice: {slice}") \
            .format(conn=objects.get('hdfs_connection'),
                    dataset=objects.get('dataset'),
                    slice=objects.get('slice'))
        return json_response(data=info)

    @catch_exception
    @expose("/offline_info/<id>/", methods=['GET'])
    def offline_info(self, id):  # Deprecated
        hdfs_conn = self.get_object(id)
        self.check_release_perm(hdfs_conn.guardian_datasource())
        objects = self.release_affect_objects(hdfs_conn)
        info = _("Changing connection {conn} to offline will make these "
                 "unusable for other users: \nDataset: {dataset}, \nSlice: {slice}") \
            .format(conn=objects.get('hdfs_connection'),
                    dataset=objects.get('dataset'),
                    slice=objects.get('slice'))
        return json_response(data=info)

    def release_affect_objects(self, hdfs_conn):  # Deprecated
        """
        Changing hdfs connection to online/offline will affect online_datasets based on this
        and online_slices based on these online_datasets
        """
        hdfs_tables = hdfs_conn.hdfs_table
        datasets = [t.dataset for t in hdfs_tables if t.dataset]
        online_datasets = [d for d in datasets if d.online is True]
        online_dataset_ids = [dataset.id for dataset in online_datasets]

        online_slices = db.session.query(Slice) \
            .filter(
                or_(Slice.datasource_id.in_(online_dataset_ids),
                    Slice.database_id == id),
                Slice.online == 1
        ).all()
        return {'hdfs_connection': [hdfs_conn, ],
                'dataset': online_datasets,
                'slice': online_slices}

    @catch_exception
    @expose("/delete_info/<id>/", methods=['GET'])
    def delete_info(self, id):
        hdfs_conn = self.get_object(id)
        self.check_delete_perm(hdfs_conn.guardian_datasource())
        objects = self.delete_affect_objects(hdfs_conn)
        info = _("Deleting connection {conn} will make these unusable: "
                 "\nDataset: {dataset}, \nSlice: {slice}") \
            .format(conn=objects.get('hdfs_connection'),
                    dataset=objects.get('dataset'),
                    slice=objects.get('slice'))
        return json_response(data=info)

    def delete_affect_objects(self, hdfs_conn):
        """
        Deleting hdfs connection will affect myself datasets and online datasets.
        myself slices and online slices based on these online_datasets
        """
        user_id = g.user.id
        hdfs_tables = hdfs_conn.hdfs_table
        datasets = [t.dataset for t in hdfs_tables if t.dataset]

        online_datasets = [d for d in datasets if d.online is True]
        myself_datasets = [d for d in datasets if d.created_by_fk == user_id]
        online_dataset_ids = [dataset.id for dataset in online_datasets]
        myself_dataset_ids = [dataset.id for dataset in myself_datasets]

        slices = db.session.query(Slice) \
            .filter(
                or_(
                    and_(
                        or_(Slice.datasource_id.in_(online_dataset_ids),
                            Slice.database_id == id),
                        Slice.online == 1
                    ),
                    and_(
                        or_(Slice.datasource_id.in_(myself_dataset_ids),
                            Slice.database_id == id),
                        Slice.created_by_fk == user_id
                    )
                )
        ).all()
        return {'hdfs_connection': [hdfs_conn, ],
                'dataset': list(set(online_datasets + myself_datasets)),
                'slice': slices
                }

    @catch_exception
    @expose("/grant_info/<id>/", methods=['GET'])
    def grant_info(self, id):
        hdfs = self.get_object(id)
        self.check_grant_perm(hdfs.guardian_datasource())
        return json_response(data="")

    @catch_hdfs_exception
    @connection_timeout
    @expose('/test/', methods=['GET'])
    def test_hdfs_connection(self):
        httpfs = request.args.get('httpfs')
        client = HDFSBrowser.login_filerobot(httpfs=httpfs)
        response = client.list('/', 1, 3)
        if response.status_code == requests.codes.ok:
            return json_response(
                message=_('Httpfs [{httpfs}] is available').format(httpfs=httpfs))
        else:
            return json_response(
                message=_('Httpfs [{httpfs}] is unavailable').format(httpfs=httpfs),
                status=500)


class ConnectionView(BaseSupersetView, PageMixin, PermissionManagement):
    """Connection includes Database and HDFSConnection.
    This view just gets the list data of Database and HDFSConnection
    """
    model = models.Connection
    model_type = 'connection'
    route_base = '/connection'

    def get_list_args(self, args):
        kwargs = super(ConnectionView, self).get_list_args(args)
        kwargs['connection_type'] = args.get('connection_type')
        return kwargs

    @catch_exception
    @expose('/connection_types/', methods=['GET', ])
    def connection_types(self):
        return json_response(data=list(Connection.connection_types))

    @catch_exception
    @expose('/listdata/', methods=['GET', ])
    def get_list_data(self):
        kwargs = self.get_list_args(request.args)
        list_data = self.get_object_list_data(**kwargs)
        return json_response(data=list_data)

    @catch_exception
    @expose('/muldelete/', methods=['POST', ])
    def muldelete(self):
        json_data = json.loads(str(request.data, encoding='utf-8'))
        json_data = {k.lower(): v for k, v in json_data.items()}
        #
        db_ids = json_data.get('database')
        if db_ids:
            objs = db.session.query(Database).filter(Database.id.in_(db_ids)).all()
            if len(db_ids) != len(objs):
                raise ParameterException(_(
                    "Error parameter ids: {ids}, queried {num} connection(s)")
                    .format(ids=db_ids, num=len(objs))
                )
            db_view = DatabaseView()
            for id in db_ids:
                db_view.delete(id)
        #
        hdfs_conn_ids = json_data.get('hdfs')
        if hdfs_conn_ids:
            objs = db.session.query(HDFSConnection) \
                .filter(HDFSConnection.id.in_(hdfs_conn_ids)).all()
            if len(hdfs_conn_ids) != len(objs):
                raise ParameterException(_(
                    "Error parameter ids: {ids}, queried {num} connection(s)")
                    .format(ids=hdfs_conn_ids, num=len(objs))
                )
            hdfs_view = HDFSConnectionModelView()
            for id in hdfs_conn_ids:
                hdfs_view.delete(id)

        return json_response(message=DELETE_SUCCESS)

    @catch_exception
    @expose("/muldelete_info/", methods=['POST'])
    def muldelete_info(self):
        """
        Deleting connections will affect myself datasets and online datasets.
        myself slices and online slices based on these online_datasets
        """
        json_data = json.loads(str(request.data, encoding='utf-8'))
        json_data = {k.lower(): v for k, v in json_data.items()}
        db_ids = json_data.get('database')
        hdfs_conn_ids = json_data.get('hdfs')
        user_id = g.user.id

        dbs, hconns, datasets, slices = [], [], [], []
        if db_ids:
            dbs = db.session.query(Database).filter(
                Database.id.in_(db_ids)
            ).all()
            if len(db_ids) != len(dbs):
                raise ParameterException(_(
                    "Error parameter ids: {ids}, queried {num} connection(s)")
                    .format(ids=db_ids, num=len(dbs))
                )
        if hdfs_conn_ids:
            hconns = db.session.query(HDFSConnection).filter(
                HDFSConnection.id.in_(hdfs_conn_ids)
            ).all()
            if len(hdfs_conn_ids) != len(hconns):
                raise ParameterException(_(
                    "Error parameter ids: {ids}, queried {num} connection(s)")
                    .format(ids=hdfs_conn_ids, num=len(hconns))
                )

        for d in dbs:
            datasets.extend(d.dataset)
        for hconn in hconns:
            for htable in hconn.hdfs_table:
                if htable.dataset:
                    datasets.append(htable.dataset)

        online_datasets = [d for d in datasets if d.online is True]
        myself_datasets = [d for d in datasets if d.created_by_fk == user_id]
        online_dataset_ids = [dataset.id for dataset in online_datasets]
        myself_dataset_ids = [dataset.id for dataset in myself_datasets]

        slices = db.session.query(Slice) \
            .filter(
                or_(
                    and_(
                        or_(Slice.datasource_id.in_(online_dataset_ids),
                            Slice.database_id == id),
                        Slice.online == 1
                    ),
                    and_(
                        or_(Slice.datasource_id.in_(myself_dataset_ids),
                            Slice.database_id == id),
                        Slice.created_by_fk == user_id
                    )
                )
        ).all()

        info = _("Deleting connection {conn} will make these unusable: "
                 "\nDataset: {dataset}, \nSlice: {slice}") \
            .format(conn=dbs + hconns,
                    dataset=list(set(online_datasets + myself_datasets)),
                    slice=slices)
        return json_response(data=info)

    def get_object_list_data(self, **kwargs):
        order_column = kwargs.get('order_column')
        order_direction = kwargs.get('order_direction')
        page = kwargs.get('page')
        page_size = kwargs.get('page_size')
        filter = kwargs.get('filter')
        user_id = kwargs.get('user_id')
        connection_type = kwargs.get('connection_type')

        s1 = select([Database.id.label('id'),
                     Database.database_name.label('name'),
                     Database.online.label('online'),
                     Database.created_by_fk.label('user_id'),
                     Database.changed_on.label('changed_on'),
                     Database.sqlalchemy_uri.label('connection_type'),
                     Database.expose.label('expose')])
        s2 = select([HDFSConnection.id.label('id'),
                     HDFSConnection.connection_name.label('name'),
                     HDFSConnection.online.label('online'),
                     HDFSConnection.created_by_fk.label('user_id'),
                     HDFSConnection.changed_on.label('changed_on'),
                     cast(literal('HDFS'), type_=sqla.String).label('connection_type'),
                     cast(literal(1), type_=sqla.Integer).label('expose')])
        union_q = s1.union_all(s2).alias('connection')
        query = (
            db.session.query(union_q, User.username)
            .outerjoin(User, User.id == union_q.c.user_id)
            .filter(union_q.c.expose == 1)
        )
        if connection_type:
            match_str = '{}%'.format(connection_type)
            query = query.filter(
                    union_q.c.connection_type.ilike(match_str)
            )
        if filter:
            filter_str = '%{}%'.format(filter.lower())
            query = query.filter(
                or_(
                    union_q.c.name.ilike(filter_str),
                    union_q.c.connection_type.ilike(filter_str),
                    User.username.ilike(filter_str)
                )
            )
        if order_column:
            try:
                column = self.str_to_column(union_q).get(order_column)
                if order_direction == 'desc':
                    query = query.order_by(column.desc())
                else:
                    query = query.order_by(column)
            except KeyError:
                msg = _('Error order column name: [{name}]').format(name=order_column)
                raise ParameterException(msg)

        global_read = True
        readable_db_names = None
        readable_hdfs_names = None
        count = 0
        if self.guardian_auth:
            from superset.guardian import guardian_client as client
            if not client.check_global_read(g.user.username):
                global_read = False
                username = g.user.username
                readable_db_names = client.search_model_perms(
                    username, Database.guardian_type)
                readable_hdfs_names = client.search_model_perms(
                    username, HDFSConnection.guardian_type)
                count = len(readable_db_names) + len(readable_hdfs_names)

        if global_read:
            count = query.count()
            if page is not None and page >= 0 and page_size and page_size > 0:
                query = query.limit(page_size).offset(page * page_size)

        rs = query.all()
        data = []
        index = 0
        for row in rs:
            type_ = row[5]
            if not global_read:
                if (type_ == 'HDFS' and row[1] in readable_hdfs_names) \
                        or (type_ != 'HDFS' and row[1] in readable_db_names):
                    index += 1
                    if index <= page * page_size:
                        continue
                    elif index > (page+1) * page_size:
                        break
                else:
                    continue

            if type_ != 'HDFS':
                url = make_url(type_)
                type_ = url.get_backend_name().upper()
            t = row[4] if row[4] else datetime(1970, 1, 1)
            data.append({
                'id': row[0],
                'name': row[1],
                'online': row[2],
                'changed_on': str(row[4]),
                'changed_time': time.mktime(t.timetuple()),
                'connection_type': type_,
                'owner': row[7],
            })

        response = {}
        response['count'] = count
        response['order_column'] = order_column
        response['order_direction'] = 'desc' if order_direction == 'desc' else 'asc'
        response['page'] = page
        response['page_size'] = page_size
        response['data'] = data
        return response

    @staticmethod
    def str_to_column(query):
        return {
            'name': query.c.name,
            'online': query.c.online,
            'changed_on': query.c.changed_on,
            'connection_type': query.c.connection_type,
            'owner': User.username
        }
