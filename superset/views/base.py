from datetime import datetime
import json
import logging
import copy
from distutils.util import strtobool
import functools
from flask import g, request, Response, redirect, abort, flash, get_flashed_messages
from flask_babel import get_locale
from flask_babel import gettext as __
from flask_babel import lazy_gettext as _
from flask_babel.speaklater import LazyString
from flask_appbuilder import ModelView, BaseView, expose
from flask_appbuilder.actions import action
from flask_appbuilder.models.sqla.filters import BaseFilter
from flask_appbuilder.security.sqla.models import User
from flask_appbuilder.widgets import ListWidget
import sqlalchemy as sqla
from sqlalchemy import and_, or_
import traceback
import yaml

from superset import app, db, models, utils, conf, appbuilder, security_manager
from superset.utils import GUARDIAN_AUTH
from superset.models import (
    Dataset, Database, Dashboard, Slice, HDFSConnection, FavStar, Log, Number
)
from superset.message import *
from superset.exceptions import (
    SupersetException2, LoginException, PermissionException, ParameterException,
    DatabaseException, PropertyException
)
from superset.translations.utils import get_language_pack


config = app.config
QueryStatus = utils.QueryStatus


FRONTEND_CONF_KEYS = (
    'PILOT_WEBSERVER_TIMEOUT',
    'ENABLE_JAVASCRIPT_CONTROLS',
)


def catch_exception(f):
    """
    A decorator to label an endpoint as an API. Catches uncaught exceptions and
    return the response in the JSON format
    """
    def wraps(self, *args, **kwargs):
        try:
            return f(self, *args, **kwargs)
        except sqla.exc.IntegrityError as ie:
            logging.exception(ie)
            if 'Duplicate entry' in str(ie):
                return json_response(status=500, message=UPDATE_FAILED, code=1)
            else:
                return json_response(status=500, message=str(ie), code=1)
        except SupersetException2 as e:
            logging.exception(e)
            return json_response(status=500, message=e.message, code=e.code)
        except AttributeError as e:
            if 'AnonymousUserMixin' in str(e):
                logging.error(e)
                return redirect(appbuilder.get_url_for_login)
            else:
                logging.exception(e)
                return json_response(status=500, message=str(e), code=1)
        except Exception as e:
            logging.exception(e)
            return json_response(status=500, message=str(e), code=1)
    return functools.update_wrapper(wraps, f)


def get_error_msg():
    if conf.get('SHOW_STACKTRACE'):
        error_msg = traceback.format_exc()
    else:
        error_msg = 'FATAL ERROR \n'
        error_msg += (
            'Stacktrace is hidden. Change the SHOW_STACKTRACE '
            'configuration setting to enable it')
    return error_msg


def json_error_response(msg=None, status=500, stacktrace=None, payload=None, link=None):
    if not payload:
        payload = {'error': str(msg)}
        if stacktrace:
            payload['stacktrace'] = stacktrace
        if link:
            payload['link'] = link
    return Response(
        json.dumps(payload, default=utils.json_iso_dttm_ser),
        status=status, mimetype='application/json')


def generate_download_headers(extension, filename=None):
    filename = filename if filename else datetime.now().strftime('%Y%m%d_%H%M%S')
    content_disp = 'attachment; filename={}.{}'.format(filename, extension)
    headers = {
        'Content-Disposition': content_disp,
    }
    return headers


def json_response(message='', status=200, data=None, code=0):
    if isinstance(message, LazyString):
        message = str(message)  # py3
    resp = {'status': status,
            'code': code,
            'message': message,
            'data': data}
    return Response(
        json.dumps(resp, default=utils.json_iso_dttm_ser, ensure_ascii=False),
        status=status,
        mimetype="application/json"
    )


def api(f):
    """
    A decorator to label an endpoint as an API. Catches uncaught exceptions and
    return the response in the JSON format
    """
    def wraps(self, *args, **kwargs):
        try:
            return f(self, *args, **kwargs)
        except Exception as e:
            logging.exception(e)
            return json_error_response(get_error_msg())

    return functools.update_wrapper(wraps, f)


def get_datasource_exist_error_mgs(full_name):
    return __('Datasource %(name)s already exists', name=full_name)


def get_user_roles():
    if g.user.is_anonymous():
        public_role = conf.get('AUTH_ROLE_PUBLIC')
        return [security_manager.find_role(public_role)] if public_role else []
    return g.user.roles


def check_ownership(obj, raise_if_false=True):
    """Meant to be used in `pre_update` hooks on models to enforce ownership

    Admin have all access, and other users need to be referenced on either
    the created_by field that comes with the ``AuditMixin``, or in a field
    named ``owners`` which is expected to be a one-to-many with the User
    model. It is meant to be used in the ModelView's pre_update hook in
    which raising will abort the update.
    """
    if not obj:
        return False

    security_exception = PermissionException(
        "You don't have the rights to alter [{}]".format(obj))

    if g.user.is_anonymous():
        if raise_if_false:
            raise security_exception
        return False
    roles = (r.name for r in get_user_roles())
    if 'Admin' in roles:
        return True
    session = db.create_scoped_session()
    orig_obj = session.query(obj.__class__).filter_by(id=obj.id).first()
    owner_names = (user.username for user in orig_obj.owners)
    if (
                    hasattr(orig_obj, 'created_by') and
                    orig_obj.created_by and
                    orig_obj.created_by.username == g.user.username):
        return True
    if (
                        hasattr(orig_obj, 'owners') and
                        g.user and
                    hasattr(g.user, 'username') and
                    g.user.username in owner_names):
        return True
    if raise_if_false:
        raise security_exception
    else:
        return False


class PermissionManagement(object):

    OBJECT_TYPES = ['database', 'hdfsconnection', 'dataset', 'slice', 'dashboard']

    def __init__(self):
        self.guardian_auth = conf.get(GUARDIAN_AUTH, False)

    def del_perm_obj(self, finite_obj):
        if self.guardian_auth:
            from superset.guardian import guardian_admin as admin
            admin.del_perm_obj(finite_obj)

    def rename_perm_obj(self, old_datasource, new_datasource):
        if old_datasource == new_datasource:
            return
        if self.guardian_auth:
            from superset.guardian import guardian_admin as admin
            logging.info('Rename permission object {} to {}'
                         .format(old_datasource, new_datasource))
            admin.rename_perm_obj(old_datasource, new_datasource)

    def grant_owner_perms(self, finite_obj):
        if self.guardian_auth:
            from superset.guardian import guardian_admin as admin
            logging.info('Grant owner permissions of {} to user [{}]'
                         .format(finite_obj, g.user.username))
            admin.grant_owner_perm(g.user.username, finite_obj)

    def grant_read_perm(self, finite_obj):
        if self.guardian_auth:
            from superset.guardian import guardian_admin as admin
            logging.info('Grant read permission of {} to user [{}]'
                         .format(finite_obj, g.user.username))
            admin.grant_read_perm(g.user.username, finite_obj)

    def check_read_perm(self, finite_obj, raise_if_false=True):
        logging.info('Check read permission of {} for user [{}]'
                     .format(finite_obj, g.user.username))
        can = self.do_check(g.user.username, finite_obj, 'check_read_access',
                            global_check_api='check_global_read')
        if not can and raise_if_false:
            raise PermissionException(
                _('No privilege to read {name}').format(name=finite_obj))
        else:
            return can

    def check_edit_perm(self, finite_obj, raise_if_false=True):
        logging.info('Check edit permission of {} for user [{}]'
                     .format(finite_obj, g.user.username))
        can = self.do_check(g.user.username, finite_obj, 'check_edit_access',
                            global_check_api='check_global_edit')
        if not can and raise_if_false:
            raise PermissionException(
                _('No privilege to edit {name}').format(name=finite_obj))
        else:
            return can

    def check_delete_perm(self, finite_obj, raise_if_false=True):
        logging.info('Check delete permission of {} for user [{}]'
                     .format(finite_obj, g.user.username))
        can = self.do_check(g.user.username, finite_obj, 'check_delete_access',
                            global_check_api='check_global_edit')
        if not can and raise_if_false:
            raise PermissionException(
                _('No privilege to delete {name}').format(name=finite_obj))
        else:
            return can

    def check_admin_perm(self, finite_obj, raise_if_false=True):
        logging.info('Check admin permission of {} for user [{}]'
                     .format(finite_obj, g.user.username))
        can = self.do_check(g.user.username, finite_obj, 'check_admin_access',
                            global_check_api='check_global_admin')
        if not can and raise_if_false:
            raise PermissionException(
                _('No privilege ADMIN of {name}').format(name=finite_obj))
        else:
            return can

    def check_grant_perm(self, finite_obj, raise_if_false=True):
        logging.info('Check grant permission of {} for user [{}]'
                     .format(finite_obj, g.user.username))
        can = self.do_check(g.user.username, finite_obj, 'check_grant_access',
                            global_check_api='check_global_admin')
        if not can and raise_if_false:
            raise PermissionException(
                _('No privilege to grant permission on {name}')
                    .format(name=finite_obj))
        else:
            return can

    def check_revoke_perm(self, finite_obj, raise_if_false=True):
        logging.info('Check revoke permission of {} for user [{}]'
                     .format(finite_obj, g.user.username))
        can = self.do_check(g.user.username, finite_obj, 'check_revoke_access',
                            global_check_api='check_global_admin')
        if not can and raise_if_false:
            raise PermissionException(
                _('No privilege to revoke permissions from {name}')
                    .format(name=finite_obj))
        else:
            return can

    def check_release_perm(self, finite_obj, raise_if_false=True):
        logging.info('Check release permission of {} for user [{}]'
                     .format(finite_obj, g.user.username))
        can = self.do_check(g.user.username, finite_obj, 'check_release_access',
                            global_check_api='check_global_admin')
        if not can and raise_if_false:
            raise PermissionException(
                _('No privilege to release {name}').format(name=finite_obj))
        else:
            return can

    def do_check(self, username, finite_obj, client_check_api, global_check_api=None):
        if self.guardian_auth:
            from superset.guardian import guardian_client as client
            if global_check_api is not None:
                can = getattr(client, global_check_api)(username)
                if can:
                    return True
            return getattr(client, client_check_api)(username, finite_obj)
        else:
            return True

    def do_grant(self, username, finite_obj, actions):
        if self.guardian_auth:
            from superset.guardian import guardian_admin as admin
            admin.grant(username, finite_obj, actions)

    def do_revoke(self, username, finite_obj, actions):
        if self.guardian_auth:
            from superset.guardian import guardian_admin as admin
            admin.revoke(username, finite_obj, actions)

    def search_object_permissions(self, finite_obj):
        if self.guardian_auth:
            from superset.guardian import guardian_client as client
            return client.search_object_permissions(finite_obj)
        else:
            return None

    def get_guardian_users(self, prefix):
        if self.guardian_auth:
            from superset.guardian import guardian_client as client
            return client.get_users(prefix)
        else:
            return []

    def init_examples_perms(self):
        """Grant READ perm of example data to present user"""
        if not self.guardian_auth:
            return
        example_types = {'DASHBOARD': Dashboard,
                         'SLICE': Slice,
                         'DATASET': Dataset,
                         'DATABASE': Database,
                         'HDFSCONNECTION': HDFSConnection}
        for obj_type, model in example_types.items():
            if obj_type == 'DATABASE':
                objs = db.session.query(model) \
                    .filter(model.created_by_fk == None,
                            model.database_name != config.get("METADATA_CONN_NAME")). \
                    all()
            else:
                objs = db.session.query(model).filter(model.created_by_fk == None).all()

            if objs and not self.check_read_perm([obj_type, objs[0].name],
                                                 raise_if_false=False):
                for obj in objs:
                    self.grant_read_perm([obj_type, obj.name])


class BaseSupersetView(BaseView):

    def __init__(self):
        super(BaseSupersetView, self).__init__()
        self.guardian_auth = conf.get(GUARDIAN_AUTH, False)
        self.main_db_name = config.get('METADATA_CONN_NAME')
        self.default_hdfs_conn_name = config.get('DEFAULT_HDFS_CONN_NAME')

    def user_id(self):
        id = g.user.get_id()
        if id:
            return int(id)
        else:
            raise LoginException(1, NO_USER)

    def get_request_data(self):
        return json.loads(str(request.data, encoding='utf-8'))

    def generate_download_headers(self, extension):
        filename = datetime.now().strftime("%Y%m%d_%H%M%S")
        content_disp = "attachment; filename={}.{}".format(filename, extension)
        headers = {
            "Content-Disposition": content_disp,
        }
        return headers

    def get_main_db(self):
        return db.session.query(Database) \
            .filter_by(database_name=self.main_db_name).first()

    def get_default_hdfs_conn(self):
        return db.session.query(HDFSConnection) \
            .filter_by(connection_name=self.default_hdfs_conn_name).one()

    def common_bootsrap_payload(self):
        """Common data always sent to the client"""
        messages = get_flashed_messages(with_categories=True)
        locale = str(get_locale())
        return {
            'flash_messages': messages,
            'conf': {k: conf.get(k) for k in FRONTEND_CONF_KEYS},
            'locale': locale,
            'language_pack': get_language_pack(locale),
        }


class SupersetListWidget(ListWidget):
    template = 'superset/fab_overrides/list.html'


class PageMixin(object):
    # used for querying
    page = 0
    page_size = 10
    order_column = 'changed_on'
    order_direction = 'desc'
    filter = None
    only_favorite = False        # all or favorite

    def get_list_args(self, args):
        kwargs = {}
        kwargs['user_id'] = g.user.id
        kwargs['order_column'] = args.get('order_column', self.order_column)
        kwargs['order_direction'] = args.get('order_direction', self.order_direction)
        kwargs['page'] = int(args.get('page', self.page))
        kwargs['page_size'] = int(args.get('page_size', self.page_size))
        kwargs['filter'] = args.get('filter', self.filter)
        fav = args.get('only_favorite')
        kwargs['only_favorite'] = strtobool(fav) if fav else self.only_favorite
        return kwargs


class SupersetModelView(ModelView):
    page_size = 100
    list_widget = SupersetListWidget


class PilotModelView(BaseSupersetView, ModelView, PageMixin, PermissionManagement):
    page_size = 100
    list_widget = SupersetListWidget
    model = models.Model
    model_type = 'model'
    # used for Data type conversion
    int_columns = []
    bool_columns = []
    str_columns = []

    def __init__(self):
        super(PilotModelView, self).__init__()
        self.guardian_auth = conf.get(GUARDIAN_AUTH, False)

    def get_list_args(self, args):
        kwargs = super(PilotModelView, self).get_list_args(args)
        kwargs['dataset_type'] = args.get('dataset_type')
        kwargs['dataset_id'] = int(args.get('dataset_id')) \
            if args.get('dataset_id') else None
        return kwargs

    @expose('/list/')
    def list(self):
        self.update_redirect()
        if g.user.get_id() is None:
            return redirect(appbuilder.get_url_for_login)
        return self.render_template(self.list_template)

    @catch_exception
    @expose('/listdata/')
    def get_list_data(self):
        kwargs = self.get_list_args(request.args)
        list_data = self.get_object_list_data(**kwargs)
        return json_response(data=list_data)

    @catch_exception
    @expose('/add/', methods=['GET', 'POST'])
    def add(self):
        json_data = self.get_request_data()
        _, obj = self.populate_object(None, g.user.id, json_data)
        self._add(obj)
        data = {'object_id': obj.id}
        return json_response(message=ADD_SUCCESS, data=data)

    def _add(self, obj):
        self.pre_add(obj)
        if not self.datamodel.add(obj):
            raise DatabaseException(ADD_FAILED)
        self.post_add(obj)

    def pre_add(self, obj):
        self.check_column_values(obj)

    def post_add(self, obj):
        self.grant_owner_perms(obj.guardian_datasource())
        Number.log_number(g.user.username, self.model_type)
        Log.log_add(obj, self.model_type, g.user.id)

    @catch_exception
    @expose('/show/<pk>/', methods=['GET'])
    def show(self, pk):
        obj = self.get_object(pk)
        attributes = self.get_show_attributes(obj, user_id=g.user.id)
        return json_response(data=attributes)

    @catch_exception
    @expose('/edit/<pk>/', methods=['POST'])
    def edit(self, pk):
        json_data = self.get_request_data()
        old_obj, new_obj = self.populate_object(pk, g.user.id, json_data)
        self._edit(old_obj, new_obj)
        return json_response(message=UPDATE_SUCCESS)

    def _edit(self, old_obj, new_obj):
        self.pre_update(old_obj, new_obj)
        if not self.datamodel.edit(new_obj):
            raise DatabaseException(UPDATE_FAILED)
        self.post_update(old_obj, new_obj)

    def pre_update(self, old_obj, new_obj):
        self.check_edit_perm(old_obj.guardian_datasource())
        self.pre_add(new_obj)

    def post_update(self, old_obj, new_obj):
        self.rename_perm_obj(old_obj.guardian_datasource(),
                             new_obj.guardian_datasource())
        Log.log_update(new_obj, self.model_type, g.user.id)

    @catch_exception
    @expose('/delete/<pk>/')
    def delete(self, pk):
        obj = self.get_object(pk)
        self._delete(obj)
        return json_response(message=DELETE_SUCCESS)

    def _delete(self, obj):
        self.pre_delete(obj)
        if not self.datamodel.delete(obj):
            raise DatabaseException(DELETE_FAILED)
        self.post_delete(obj)

    def pre_delete(self, obj):
        self.check_delete_perm(obj.guardian_datasource())

    def post_delete(self, obj):
        self.del_perm_obj(obj.guardian_datasource())
        Number.log_number(g.user.username, self.model_type)
        Log.log_delete(obj, self.model_type, g.user.id)

    @catch_exception
    @expose('/muldelete/', methods=['GET', 'POST'])
    def muldelete(self):
        json_data = self.get_request_data()
        ids = json_data.get('selectedRowKeys')
        objs = db.session.query(self.model).filter(self.model.id.in_(ids)).all()
        if len(ids) != len(objs):
            raise ParameterException(
                "Error parameter ids: {}, get {} object(s) in database"
                    .format(ids, len(objs))
            )
        for obj in objs:
            self._delete(obj)
        return json_response(message=DELETE_SUCCESS)

    def get_object_list_data(self, **kwargs):
        pass

    def populate_object(self, obj_id, user_id, data):
        old_obj = None
        if obj_id:
            new_obj = self.get_object(obj_id)
            old_obj = copy.deepcopy(new_obj)
            attributes = self.get_edit_attributes(data, user_id)
        else:
            new_obj = self.model()
            attributes = self.get_add_attributes(data, user_id)
        for key, value in attributes.items():
            if isinstance(value, str):
                setattr(new_obj, key, value.strip())
            else:
                setattr(new_obj, key, value)
        return old_obj, new_obj

    def get_show_attributes(self, obj, user_id=None):
        attributes = {}
        for col in self.show_columns:
            if not hasattr(obj, col) and not hasattr(obj, '"{}"'.format(col)):
                msg = _("Class [{cls}] does not have the attribute [{attribute}]") \
                    .format(cls=obj.__class__.__name__, attribute=col)
                self.handle_exception(500, KeyError, msg)
            if col in self.str_columns:
                attributes[col] = str(getattr(obj, col, None))
            else:
                attributes[col] = getattr(obj, col, None)

        # attributes['readme'] = self.get_column_readme()
        attributes['created_by_user'] = obj.created_by.username \
            if obj.created_by else None
        attributes['changed_by_user'] = obj.changed_by.username \
            if obj.changed_by else None
        return attributes

    def get_add_attributes(self, data, user_id):
        attributes = {}
        for col in self.add_columns:
            if col not in data:
                msg = _("The needed attribute [{attribute}] is not in attributes [{attributes}]") \
                    .format(attribute=col, attributes=','.join(data.keys()))
                self.handle_exception(404, KeyError, msg)
            value = data.get(col)
            if col in self.bool_columns and not isinstance(value, bool):
                attributes[col] = strtobool(value)
            elif col in self.int_columns:
                attributes[col] = int(value)
            else:
                attributes[col] = value
        return attributes

    def get_edit_attributes(self, data, user_id):
        attributes = {}
        for col in self.edit_columns:
            if col not in data:
                msg = _("The needed attribute [{attribute}] is not in attributes [{attributes}]") \
                    .format(attribute=col, attributes=','.join(data.keys()))
                self.handle_exception(404, KeyError, msg)
            value = data.get(col)
            if col in self.bool_columns and not isinstance(value, bool):
                attributes[col] = strtobool(value)
            elif col in self.int_columns:
                attributes[col] = int(value)
            else:
                attributes[col] = value
        return attributes

    def query_with_favorite(self, class_name, **kwargs):
        """
        A query api suitable for dashboard and slice
        :param class_name: 'dashboard' or 'slice'
        :param kwargs:
        :return:
        """
        user_id = kwargs.get('user_id')
        order_column = kwargs.get('order_column')
        order_direction = kwargs.get('order_direction')
        filter = kwargs.get('filter')
        only_favorite = kwargs.get('only_favorite')

        query = (
            db.session.query(self.model, User.username, FavStar.obj_id)
                .outerjoin(User, self.model.created_by_fk == User.id)
        )
        if only_favorite:
            query = query.join(
                FavStar,
                and_(
                    self.model.id == FavStar.obj_id,
                    FavStar.class_name.ilike(class_name),
                    FavStar.user_id == user_id)
            )
        else:
            query = query.outerjoin(
                FavStar,
                and_(
                    self.model.id == FavStar.obj_id,
                    FavStar.class_name.ilike(class_name),
                    FavStar.user_id == user_id)
            )

        if filter:
            filter_str = '%{}%'.format(filter.lower())
            if class_name.lower() == Dashboard.model_type:
                query = query.filter(
                    or_(Dashboard.name.ilike(filter_str),
                        User.username.ilike(filter_str))
                )
            elif class_name.lower() == Slice.model_type:
                query = query.filter(
                    or_(Slice.slice_name.ilike(filter_str),
                        User.username.ilike(filter_str))
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
        return query

    @staticmethod
    def dashboards_to_dict(dashs):
        dashs_list = []
        for dash in dashs:
            row = {'id': dash.id, 'name': dash.name}
            dashs_list.append(row)
        return dashs_list

    @staticmethod
    def slices_to_dict(slices):
        slices_list = []
        for slice in slices:
            row = {'id': slice.id,
                   'slice_name': slice.slice_name,
                   'viz_type': slice.viz_type}
            slices_list.append(row)
        return slices_list

    def get_object(self, obj_id):
        try:
            obj_id = int(obj_id)
        except Exception as e:
            raise PropertyException("[{}] is not a valid id, may this {} is not existed"
                                    .format(obj_id, self.model.__name__))
        try:
            obj = db.session.query(self.model).filter_by(id=obj_id).one()
        except sqla.orm.exc.NoResultFound:
            msg = _("Not found the object: model={model}, id={id}") \
                .format(model=self.model.__name__, id=obj_id)
            self.handle_exception(500, sqla.orm.exc.NoResultFound, msg)
        else:
            return obj

    def handle_exception(self, status, exception, msg):
        self.status = status
        logging.error(msg)
        raise exception(msg)

    def check_online(self, obj, raise_if_false=True):
        online = getattr(obj, 'online', False)
        if online and raise_if_false:
            raise PermissionException(_('Can not edit a online object'))
        else:
            return online


class ListWidgetWithCheckboxes(ListWidget):
    """An alternative to list view that renders Boolean fields as checkboxes

    Works in conjunction with the `checkbox` view."""
    template = 'superset/fab_overrides/list_with_checkboxes.html'


def validate_json(form, field):  # noqa
    try:
        json.loads(field.data)
    except Exception as e:
        logging.exception(e)
        raise Exception(_("json isn't valid"))


class YamlExportMixin(object):
    @action('yaml_export', __('Export to YAML'), __('Export to YAML?'), 'fa-download')
    def yaml_export(self, items):
        if not isinstance(items, list):
            items = [items]

        data = [t.export_to_dict() for t in items]
        return Response(
            yaml.safe_dump(data),
            headers=generate_download_headers('yaml'),
            mimetype='application/text')


class DeleteMixin(object):
    def _delete(self, pk):
        """Delete function logic, override to implement diferent logic
        deletes the record with primary_key = pk

        :param pk: record primary key to delete
        """
        item = self.datamodel.get(pk, self._base_filters)
        if not item:
            abort(404)
        try:
            self.pre_delete(item)
        except Exception as e:
            flash(str(e), 'danger')
        else:
            view_menu = security_manager.find_view_menu(item.get_perm())
            pvs = security_manager.get_session.query(
                security_manager.permissionview_model).filter_by(
                view_menu=view_menu).all()

            schema_view_menu = None
            if hasattr(item, 'schema_perm'):
                schema_view_menu = security_manager.find_view_menu(item.schema_perm)

                pvs.extend(security_manager.get_session.query(
                    security_manager.permissionview_model).filter_by(
                    view_menu=schema_view_menu).all())

            if self.datamodel.delete(item):
                self.post_delete(item)
                for pv in pvs:
                    security_manager.get_session.delete(pv)
                if view_menu:
                    security_manager.get_session.delete(view_menu)
                if schema_view_menu:
                    security_manager.get_session.delete(schema_view_menu)
                security_manager.get_session.commit()

            flash(*self.datamodel.message)
            self.update_redirect()

    @action('muldelete', _('Delete'), _('Delete all Really?'), 'fa-trash',
            single=False)
    def muldelete(self, items):
        if not items:
            abort(404)
        for item in items:
            try:
                self.pre_delete(item)
            except Exception as e:
                flash(str(e), 'danger')
            else:
                self._delete(item.id)
        self.update_redirect()
        return redirect(self.get_redirect())


class SupersetFilter(BaseFilter):

    """Add utility function to make BaseFilter easy and fast

    These utility function exist in the SecurityManager, but would do
    a database round trip at every check. Here we cache the role objects
    to be able to make multiple checks but query the db only once
    """

    def get_user_roles(self):
        return get_user_roles()

    def get_all_permissions(self):
        """Returns a set of tuples with the perm name and view menu name"""
        perms = set()
        for role in self.get_user_roles():
            for perm_view in role.permissions:
                t = (perm_view.permission.name, perm_view.view_menu.name)
                perms.add(t)
        return perms

    def has_role(self, role_name_or_list):
        """Whether the user has this role name"""
        if not isinstance(role_name_or_list, list):
            role_name_or_list = [role_name_or_list]
        return any(
            [r.name in role_name_or_list for r in self.get_user_roles()])

    def has_perm(self, permission_name, view_menu_name):
        """Whether the user has this perm"""
        return (permission_name, view_menu_name) in self.get_all_permissions()

    def get_view_menus(self, permission_name):
        """Returns the details of view_menus for a perm name"""
        vm = set()
        for perm_name, vm_name in self.get_all_permissions():
            if perm_name == permission_name:
                vm.add(vm_name)
        return vm

    def has_all_datasource_access(self):
        return (
            self.has_role(['Admin', 'Alpha']) or
            self.has_perm('all_datasource_access', 'all_datasource_access'))


class DatasourceFilter(SupersetFilter):
    def apply(self, query, func):  # noqa
        if self.has_all_datasource_access():
            return query
        perms = self.get_view_menus('datasource_access')
        # TODO(bogdan): add `schema_access` support here
        return query.filter(self.model.perm.in_(perms))


class CsvResponse(Response):
    """
    Override Response to take into account csv encoding from config.py
    """
    charset = conf.get('CSV_EXPORT').get('encoding', 'utf-8')


