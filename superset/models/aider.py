import re
from datetime import datetime, date
from flask_appbuilder import Model
import sqlalchemy as sqla
from sqlalchemy import (
    Column, Integer, String, ForeignKey, Text, DateTime, Date
)
from sqlalchemy.orm import relationship

from superset import app, db, security_manager
from superset import utils
from superset.utils import GUARDIAN_AUTH
from superset.exceptions import PropertyException
from superset.connector_registry import ConnectorRegistry
from .base import AuditMixinNullable
from .dataset import Dataset, TableColumn, SqlMetric
from .connection import Database, HDFSConnection, Connection
from .slice import Slice
from .dashboard import Dashboard


config = app.config

str_to_model = {
    'slice': Slice,
    'dashboard': Dashboard,
    'dataset': Dataset,
    'database': Database,
    'hdfsconnection': HDFSConnection,
    'connection': Connection
}

model_name_columns = {
    'slice': Slice.slice_name,
    'dashboard': Dashboard.name,
    'dataset': Dataset.dataset_name,
    'tablecolumn': TableColumn.column_name,
    'sqlmetric': SqlMetric.metric_name,
    'database': Database.database_name,
    'hdfsconnection': HDFSConnection.connection_name,
}


class Log(Model):
    """ORM object used to log Superset actions to the database
       Object type: ['slice', 'dashboard', 'dataset', database', 'hdfsconnection']
       Action type: ['add', 'update', 'delete', 'online', 'offline', 'import',
                      'export', 'like', 'dislike']
    """
    __tablename__ = 'logs'

    id = Column(Integer, primary_key=True)
    action = Column(String(512))
    action_type = Column(String(32))
    obj_type = Column(String(32))
    obj_id = Column(Integer)
    user_id = Column(Integer, ForeignKey('ab_user.id'))
    username = Column(String(128))
    json = Column(Text)
    user = relationship('User', backref='logs', foreign_keys=[user_id])
    dttm = Column(DateTime, default=datetime.now)
    dt = Column(Date, default=date.today())
    duration_ms = Column(Integer)
    referrer = Column(String(1024))

    record_action_types = ['online', 'offline', 'add', 'delete', 'grant', 'revoke']

    @classmethod
    def log_action(cls, action_type, action, obj_type, obj_id, user_id, username=None):
        if action_type not in cls.record_action_types:
            return
        log = cls(
            action=action,
            action_type=action_type,
            obj_type=obj_type,
            obj_id=obj_id,
            user_id=user_id,
            username=username)
        db.session().add(log)
        db.session().commit()

    @classmethod
    def log(cls, action_type, obj, obj_type, user_id):
        uniform_type = cls.convert_type(obj_type)
        action_str = '{} {}: [{}]'.format(action_type.capitalize(), uniform_type, repr(obj))
        cls.log_action(action_type, action_str, obj_type, obj.id, user_id)

    @classmethod
    def log_add(cls, obj, obj_type, user_id):
        cls.log('add', obj, obj_type, user_id)
        if hasattr(obj, 'online') and obj.online is True:
            cls.log_online(obj, obj_type, user_id)

    @classmethod
    def log_update(cls, obj, obj_type, user_id):
        cls.log('update', obj, obj_type, user_id)

    @classmethod
    def log_delete(cls, obj, obj_type, user_id):
        if hasattr(obj, 'online') and obj.online is True:
            cls.log_offline(obj, obj_type, user_id)
        cls.log('delete', obj, obj_type, user_id)

    @classmethod
    def log_online(cls, obj, obj_type, user_id):
        cls.log('online', obj, obj_type, user_id)

    @classmethod
    def log_offline(cls, obj, obj_type, user_id):
        cls.log('offline', obj, obj_type, user_id)

    @classmethod
    def log_grant(cls, obj, obj_type, user_id, username, actions):
        """The username is the user be granted"""
        action_str = 'Grant {user} {actions} on {obj_type}: [{obj_name}]'\
            .format(user=username, actions=actions, obj_type=obj_type, obj_name=repr(obj))
        cls.log_action('grant', action_str, obj_type, obj.id, user_id, username)

    @classmethod
    def log_revoke(cls, obj, obj_type, user_id, username, actions):
        """The user is the user be revoked"""
        action_str = 'Revoke {user} {actions} from {obj_type}: [{obj_name}]' \
            .format(user=username, actions=actions, obj_type=obj_type, obj_name=repr(obj))
        cls.log_action('revoke', action_str, obj_type, obj.id, user_id, username)

    @classmethod
    def convert_type(cls, obj_type):
        if obj_type in ['database', 'hdfsconnection']:
            return 'connection'
        else:
            return obj_type


class FavStar(Model):
    __tablename__ = 'favstar'

    id = Column(Integer, primary_key=True)
    user_id = Column(Integer, ForeignKey('ab_user.id'))
    class_name = Column(String(32))
    obj_id = Column(Integer)
    dttm = Column(DateTime, default=datetime.utcnow)


class Number(Model):
    """ORM object used to log objects' number readable for user
       Object type: ['slice', 'dashboard', 'dataset', 'connection']
    """
    __tablename__ = 'number'

    id = Column(Integer, primary_key=True)
    username = Column(String(128))
    obj_type = Column(String(32))
    dt = Column(Date, default=date.today())
    count = Column(Integer)

    LOG_TYPES = ['dashboard', 'slice', 'dataset', 'connection']
    OBJECT_TYPES = ['dashboard', 'slice', 'dataset', 'database', 'hdfsconnection']

    @classmethod
    def do_log(cls, username, obj_type, count):
        today_number = db.session.query(Number)\
            .filter(
                Number.username == username,
                Number.obj_type == obj_type,
                Number.dt == date.today()
            ).first()
        if today_number:
            today_number.count = count
            db.session.merge(today_number)
        else:
            today_number = cls(
                username=username,
                obj_type=obj_type,
                dt=date.today(),
                count=count,
            )
            db.session.add(today_number)
        db.session.commit()

    @classmethod
    def object_count(cls, username, obj_type):
        if obj_type not in cls.LOG_TYPES:
            raise PropertyException(
                'Error object type: [] when logging number'.format(obj_type))
        if config.get(GUARDIAN_AUTH):
            from superset.guardian import guardian_client as client
            if not client.check_global_read(username):
                if obj_type.lower() == cls.LOG_TYPES[3]:
                    db_names = client.search_model_perms(username, cls.OBJECT_TYPES[3])
                    hdfs_names = client.search_model_perms(username, cls.OBJECT_TYPES[4])
                    return len(db_names) + len(hdfs_names)
                else:
                    names = client.search_model_perms(username, obj_type)
                    return len(names)

        if obj_type.lower() == cls.LOG_TYPES[3]:
            return Database.count() + HDFSConnection.count()
        else:
            model = str_to_model.get(obj_type)
            return model.count()

    @classmethod
    def log_number(cls, username, obj_type):
        obj_type = cls.convert_type(obj_type)
        count = cls.object_count(username, obj_type)
        cls.do_log(username, obj_type, count)

    @classmethod
    def log_dashboard_number(cls, username):
        cls.log_number(username, cls.LOG_TYPES[0])

    @classmethod
    def log_slice_number(cls, username):
        cls.log_number(username, cls.LOG_TYPES[1])

    @classmethod
    def log_dataset_number(cls, username):
        cls.log_number(username, cls.LOG_TYPES[2])

    @classmethod
    def log_connection_number(cls, username):
        cls.log_number(username, cls.LOG_TYPES[3])

    @classmethod
    def convert_type(cls, obj_type):
        obj_type = obj_type.lower()
        if obj_type in cls.OBJECT_TYPES[3:]:
            obj_type = cls.LOG_TYPES[3]
        return obj_type


class Url(Model, AuditMixinNullable):
    """Used for the short url feature"""

    __tablename__ = 'url'
    id = Column(Integer, primary_key=True)
    url = Column(Text)


class KeyValue(Model):

    """Used for any type of key-value store"""

    __tablename__ = 'keyvalue'
    id = Column(Integer, primary_key=True)
    value = Column(Text, nullable=False)


class CssTemplate(Model, AuditMixinNullable):

    """CSS templates for dashboards"""

    __tablename__ = 'css_templates'
    id = Column(Integer, primary_key=True)
    template_name = Column(String(250))
    css = Column(Text, default='')


class DatasourceAccessRequest(Model, AuditMixinNullable):
    """ORM model for the access requests for datasources and dbs."""
    __tablename__ = 'access_request'
    id = Column(Integer, primary_key=True)

    datasource_id = Column(Integer)
    datasource_type = Column(String(200))

    ROLES_BLACKLIST = set(config.get('ROBOT_PERMISSION_ROLES', []))

    @property
    def cls_model(self):
        return ConnectorRegistry.sources[self.datasource_type]

    @property
    def username(self):
        return self.creator()

    @property
    def datasource(self):
        return self.get_datasource

    @datasource.getter
    @utils.memoized
    def get_datasource(self):
        # pylint: disable=no-member
        ds = db.session.query(self.cls_model).filter_by(
            id=self.datasource_id).first()
        return ds

    @property
    def datasource_link(self):
        return self.datasource.link  # pylint: disable=no-member

    @property
    def roles_with_datasource(self):
        action_list = ''
        perm = self.datasource.perm  # pylint: disable=no-member
        pv = security_manager.find_permission_view_menu('datasource_access', perm)
        for r in pv.role:
            if r.name in self.ROLES_BLACKLIST:
                continue
            url = (
                '/superset/approve?datasource_type={self.datasource_type}&'
                'datasource_id={self.datasource_id}&'
                'created_by={self.created_by.username}&role_to_grant={r.name}'
                    .format(**locals())
            )
            href = '<a href="{}">Grant {} Role</a>'.format(url, r.name)
            action_list = action_list + '<li>' + href + '</li>'
        return '<ul>' + action_list + '</ul>'

    @property
    def user_roles(self):
        action_list = ''
        for r in self.created_by.roles:  # pylint: disable=no-member
            url = (
                '/superset/approve?datasource_type={self.datasource_type}&'
                'datasource_id={self.datasource_id}&'
                'created_by={self.created_by.username}&role_to_extend={r.name}'
                    .format(**locals())
            )
            href = '<a href="{}">Extend {} Role</a>'.format(url, r.name)
            if r.name in self.ROLES_BLACKLIST:
                href = '{} Role'.format(r.name)
            action_list = action_list + '<li>' + href + '</li>'
        return '<ul>' + action_list + '</ul>'
