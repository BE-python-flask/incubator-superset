import functools
from flask import request, g
from flask_babel import lazy_gettext as _
from flask_appbuilder import expose
from superset import db, app
from superset.utils import GUARDIAN_AUTH
from superset.models import Database, str_to_model, Log, Number, model_name_columns
from superset.message import DISABLE_GAURDIAN
from superset.exception import ParameterException, PermissionException
from .base import BaseSupersetView, PermissionManagement, catch_exception, json_response

config = app.config


def guardian_entry(f):
    """
    A decorator to label an endpoint as an API. Catches uncaught exceptions and
    return the response in the JSON format
    """
    def wraps(self, *args, **kwargs):
        if not config.get(GUARDIAN_AUTH):
            raise PermissionException(DISABLE_GAURDIAN)
        return f(self, *args, **kwargs)

    return functools.update_wrapper(wraps, f)


class GuardianView(BaseSupersetView, PermissionManagement):
    route_base = '/guardian'
    OBJECT_TYPES = ['dashboard', 'slice', 'dataset', 'database', 'hdfsconnection']
    ALL_PERMS = ['READ', 'EDIT', 'ADMIN']

    @catch_exception
    @guardian_entry
    @expose('/users/', methods=['GET'])
    def get_users(self):
        prefix = request.args.get('prefix')
        users = self.get_guardian_users(prefix)
        return json_response(data={'usernames': users})

    @catch_exception
    @guardian_entry
    @expose('/permission/types/', methods=['GET'])
    def permission_types(self):
        return json_response(data={'permissions': self.ALL_PERMS})

    @catch_exception
    @guardian_entry
    @expose('/permission/search/', methods=['POST'])
    def search_permissions(self):
        """
        Search user's permission on specific object. Format of request.data is as bellow:
        {
            "object_type": "database",
            "object_name": "name"
        }
        """
        args = self.get_request_data()
        object_type = args.get('object_type')
        object_name = args.get('object_name')
        data = self.search_object_permissions([object_type, object_name])
        return json_response(data=data)

    @catch_exception
    @guardian_entry
    @expose('/permission/grant/', methods=['POST'])
    def grant_permissions(self):
        """
        Grant a user actions on a object. Format of request.data is as bellow:
        {
            "username": "a",
            "object_type": "database",
            "object_name": "name",
            "actions": ["READ", "EDIT", "ADMIN"]
        }
        """
        args = self.get_request_data()
        username = args.get('username')
        object_type = args.get('object_type')
        object_name = args.get('object_name')
        actions = args.get('actions')
        self.check_actions(actions)
        self.check_grant_perm([object_type, object_name])
        obj = self.get_object(object_type, object_name)
        self.grant_relations(username, obj, object_type, actions)
        msg = _("Grant [{}] actions {} on {} and dependencies success.") \
            .format(username, actions, [object_type, object_name])
        return json_response(message=msg)

    def grant_relations(self, username, obj, object_type, actions):
        if not self.check_grant_perm([object_type, obj.name], raise_if_false=False):
            return
        self.do_grant(username, [object_type, obj.name], actions)
        Log.log_grant(obj, object_type, g.user.id, username, actions)
        Number.log_number(username, object_type)

        main_db = self.get_main_db()
        if object_type == self.OBJECT_TYPES[0]:
            for slice in obj.slices:
                self.grant_relations(username, slice, self.OBJECT_TYPES[1], actions)
        elif object_type == self.OBJECT_TYPES[1]:
            if obj.datasource_id and obj.datasource:
                self.grant_relations(
                    username, obj.datasource, self.OBJECT_TYPES[2], actions)
            elif obj.database_id and obj.database_id != main_db.id:
                database = db.session.query(Database)\
                    .filter_by(id=obj.database_id).first()
                self.grant_relations(
                    username, database, self.OBJECT_TYPES[3], actions)
        elif object_type == self.OBJECT_TYPES[2]:
            if obj.database and obj.database.id != main_db.id:
                self.grant_relations(
                    username, obj.database, self.OBJECT_TYPES[3], actions)
            if obj.hdfs_table and obj.hdfs_table.hdfs_connection:
                self.grant_relations(
                    username, obj.hdfs_table.hdfs_connection, self.OBJECT_TYPES[4], actions)

    @catch_exception
    @guardian_entry
    @expose('/permission/revoke/', methods=['POST'])
    def revoke_permissions(self):
        """"
        Revoke a user's actions from a object. Format of request.data is as bellow:
        {
            "username": "a",
            "object_type": "database",
            "object_name": "name",
            "actions": ["READ", "EDIT", "ADMIN"]
        }
        """
        args = self.get_request_data()
        username = args.get('username')
        object_type = args.get('object_type')
        object_name = args.get('object_name')
        actions = args.get('actions')
        if username == g.user.username:
            raise PermissionException(_('Can revoke your own permissions'))
        self.check_actions(actions)
        self.check_revoke_perm([object_type, object_name])
        self.do_revoke(username, [object_type, object_name], actions)
        obj = self.get_object(object_type, object_name)
        Log.log_revoke(obj, object_type, g.user.id, username, actions)
        Number.log_number(username, object_type)
        return json_response(message="Revoke [{}] actions {} from object {} success."
                             .format(username, actions, [object_type, object_name]))

    def get_object(self, obj_type, obj_name):
        model = str_to_model.get(obj_type)
        name_column = model_name_columns.get(obj_type)
        obj = db.session.query(model).filter(name_column == obj_name).first()
        if not obj:
            raise ParameterException(_("Not found the {model} by name: [{name}]")
                                     .format(model=obj_type, name=obj_name))
        return obj

    def check_actions(self, actions):
        if isinstance(actions, list):
            if not set(actions).issubset(set(self.ALL_PERMS)):
                raise PermissionException(
                    _('Error permission action: {action}').format(action=actions))
        else:
            if actions not in self.ALL_PERMS:
                raise PermissionException(
                    _('Error permission action: {action}').format(action=actions))
