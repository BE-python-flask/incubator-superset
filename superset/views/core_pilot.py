"""Views for Pilot"""
import json
import logging
import pandas as pd
import sys
import time
import traceback
import zlib
from datetime import datetime, timedelta
from flask import (
    g, request, redirect, flash, Response, Markup, url_for, render_template
)
from flask_babel import gettext as __
from flask_babel import lazy_gettext as _
from flask_appbuilder import expose
from flask_appbuilder.models.sqla.interface import SQLAInterface
import sqlalchemy as sqla
from sqlalchemy import and_, create_engine, or_, update
from sqlalchemy.engine.url import make_url
from urllib import parse
from urllib.parse import quote
from werkzeug.routing import BaseConverter

from superset import (
    app, db, appbuilder, sql_lab, results_backend, viz, utils, security_manager
)
from superset import simple_cache as cache
from superset.exceptions import (
    PropertyException, DatabaseException, ErrorRequestException,
    SupersetException2, PermissionException
)
from superset.jinja_context import get_template_processor
from superset.legacy import cast_form_data
from superset.message import *
from superset.models import (
    Database, Dataset, Slice, Dashboard, TableColumn, SqlMetric, Query, Log,
    FavStar, str_to_model, Number, Url, KeyValue, CssTemplate, DatasourceAccessRequest,
    AnnotationDatasource
)
from superset.utils import merge_extra_filters, merge_request_params
from superset.connector_registry import ConnectorRegistry
from superset.sql_parse import SupersetQuery
from superset.timeout_decorator import connection_timeout

from .base import (
    BaseSupersetView, SupersetModelView, DeleteMixin, PermissionManagement,
    catch_exception, json_response, json_error_response, get_error_msg, CsvResponse,
    generate_download_headers, get_user_roles, check_ownership
)
from .hdfs import HDFSBrowser
from .dashboard import DashboardModelView
from .utils import bootstrap_user_data
from .slice import SliceModelView


config = app.config
QueryStatus = utils.QueryStatus
stats_logger = config.get('STATS_LOGGER')
DAR = DatasourceAccessRequest


ALL_DATASOURCE_ACCESS_ERR = __(
    'This endpoint requires the `all_datasource_access` permission')
DATASOURCE_MISSING_ERR = __('The datasource seems to have been deleted')
ACCESS_REQUEST_MISSING_ERR = __(
    'The access requests seem to have been deleted')
USER_MISSING_ERR = __('The user seems to have been deleted')
DATASOURCE_ACCESS_ERR = __("You don't have access to this datasource")

FORM_DATA_KEY_BLACKLIST = []
if not config.get('ENABLE_JAVASCRIPT_CONTROLS'):
    FORM_DATA_KEY_BLACKLIST = [
        'js_tooltip',
        'js_onclick_href',
        'js_data_mutator',
    ]


def get_database_access_error_msg(database_name):
    return __('This view requires the database %(name)s or '
              '`all_datasource_access` permission', name=database_name)


def get_datasource_access_error_msg(datasource_name):
    return __('This endpoint requires the datasource %(name)s, database or '
              '`all_datasource_access` permission', name=datasource_name)


def json_success(json_msg, status=200):
    return Response(json_msg, status=status, mimetype='application/json')


def is_owner(obj, user):
    """ Check if user is owner of the slice """
    return obj and user in obj.owners


if config.get('ENABLE_ACCESS_REQUEST'):
    class AccessRequestsModelView(SupersetModelView, DeleteMixin):
        datamodel = SQLAInterface(DAR)
        list_columns = [
            'username', 'user_roles', 'datasource_link',
            'roles_with_datasource', 'created_on']
        order_columns = ['created_on']
        base_order = ('changed_on', 'desc')
        label_columns = {
            'username': _('User'),
            'user_roles': _('User Roles'),
            'database': _('Database URL'),
            'datasource_link': _('Datasource'),
            'roles_with_datasource': _('Roles to grant'),
            'created_on': _('Created On'),
        }

    appbuilder.add_view(
        AccessRequestsModelView,
        'Access requests',
        label=__('Access requests'),
        category='Security',
        category_label=__('Security'),
        icon='fa-table')


class LogModelView(SupersetModelView):
    datamodel = SQLAInterface(Log)
    list_columns = ('user', 'action', 'dttm')
    edit_columns = ('user', 'action', 'dttm', 'json')
    base_order = ('dttm', 'desc')
    label_columns = {
        'user': _('User'),
        'action': _('Action'),
        'dttm': _('dttm'),
        'json': _('JSON'),
    }


class KV(BaseSupersetView):

    """Used for storing and retrieving key value pairs"""

    @expose('/store/', methods=['POST'])
    def store(self):
        try:
            value = request.form.get('data')
            obj = KeyValue(value=value)
            db.session.add(obj)
            db.session.commit()
        except Exception as e:
            return json_error_response(e)
        return Response(
            json.dumps({'id': obj.id}),
            status=200)

    @expose('/<key_id>/', methods=['GET'])
    def get_value(self, key_id):
        kv = None
        try:
            kv = db.session.query(KeyValue).filter_by(id=key_id).one()
        except Exception as e:
            return json_error_response(e)
        return Response(kv.value, status=200)


class R(BaseSupersetView):

    """used for short urls"""

    @expose('/<url_id>')
    def index(self, url_id):
        url = db.session.query(Url).filter_by(id=url_id).first()
        if url:
            return redirect('/' + url.url)
        else:
            flash('URL to nowhere...', 'danger')
            return redirect('/')

    @expose('/shortner/', methods=['POST', 'GET'])
    def shortner(self):
        url = request.form.get('data')
        obj = Url(url=url)
        db.session.add(obj)
        db.session.commit()
        return Response(
            '{scheme}://{request.headers[Host]}/r/{obj.id}'.format(
                scheme=request.scheme, request=request, obj=obj),
            mimetype='text/plain')

    @expose('/msg/')
    def msg(self):
        """Redirects to specified url while flash a message"""
        flash(Markup(request.args.get('msg')), 'info')
        return redirect(request.args.get('url'))


class CssTemplateModelView(SupersetModelView, DeleteMixin):
    datamodel = SQLAInterface(CssTemplate)
    list_columns = ['template_name']
    edit_columns = ['template_name', 'css']
    add_columns = edit_columns
    label_columns = {
        'template_name': _('Template Name'),
    }


class CssTemplateAsyncModelView(CssTemplateModelView):
    list_columns = ['template_name', 'css']


class Superset(BaseSupersetView, PermissionManagement):

    def json_response(self, obj, status=200):
        return Response(
            json.dumps(obj, default=utils.json_int_dttm_ser),
            status=status,
            mimetype='application/json')

    @expose('/datasources/')
    def datasources(self):
        datasources = ConnectorRegistry.get_all_datasources(db.session)
        datasources = [o.short_data for o in datasources]
        datasources = sorted(datasources, key=lambda o: o['name'])
        return self.json_response(datasources)

    @expose('/override_role_permissions/', methods=['POST'])
    def override_role_permissions(self):
        """Updates the role with the give datasource permissions.

          Permissions not in the request will be revoked. This endpoint should
          be available to admins only. Expects JSON in the format:
           {
            'role_name': '{role_name}',
            'database': [{
                'datasource_type': '{table|druid}',
                'name': '{database_name}',
                'schema': [{
                    'name': '{schema_name}',
                    'datasources': ['{datasource name}, {datasource name}']
                }]
            }]
        }
        """
        data = request.get_json(force=True)
        role_name = data['role_name']
        databases = data['database']

        db_ds_names = set()
        for dbs in databases:
            for schema in dbs['schema']:
                for ds_name in schema['datasources']:
                    fullname = utils.get_datasource_full_name(
                        dbs['name'], ds_name, schema=schema['name'])
                    db_ds_names.add(fullname)

        existing_datasources = ConnectorRegistry.get_all_datasources(db.session)
        datasources = [
            d for d in existing_datasources if d.full_name in db_ds_names]
        role = security_manager.find_role(role_name)
        # remove all permissions
        role.permissions = []
        # grant permissions to the list of datasources
        granted_perms = []
        for datasource in datasources:
            view_menu_perm = security_manager.find_permission_view_menu(
                view_menu_name=datasource.perm,
                permission_name='datasource_access')
            # prevent creating empty permissions
            if view_menu_perm and view_menu_perm.view_menu:
                role.permissions.append(view_menu_perm)
                granted_perms.append(view_menu_perm.view_menu.name)
        db.session.commit()
        return self.json_response({
            'granted': granted_perms,
            'requested': list(db_ds_names),
        }, status=201)

    @expose('/request_access/')
    def request_access(self):
        datasources = set()
        dashboard_id = request.args.get('dashboard_id')
        if dashboard_id:
            dash = (
                db.session.query(Dashboard)
                    .filter_by(id=int(dashboard_id))
                    .one()
            )
            datasources |= dash.datasources
        datasource_id = request.args.get('datasource_id')
        datasource_type = request.args.get('datasource_type')
        if datasource_id:
            ds_class = ConnectorRegistry.sources.get(datasource_type)
            datasource = (
                db.session.query(ds_class)
                    .filter_by(id=int(datasource_id))
                    .one()
            )
            datasources.add(datasource)

        has_access = all(
            (
                datasource and security_manager.datasource_access(datasource)
                for datasource in datasources
            ))
        if has_access:
            return redirect('/superset/dashboard/{}'.format(dashboard_id))

        if request.args.get('action') == 'go':
            for datasource in datasources:
                access_request = DAR(
                    datasource_id=datasource.id,
                    datasource_type=datasource.type)
                db.session.add(access_request)
                db.session.commit()
            flash(__('Access was requested'), 'info')
            return redirect('/')

        return self.render_template(
            'superset/request_access.html',
            datasources=datasources,
            datasource_names=', '.join([o.name for o in datasources]),
        )

    @expose('/approve')
    def approve(self):
        def clean_fulfilled_requests(session):
            for r in session.query(DAR).all():
                datasource = ConnectorRegistry.get_datasource(
                    r.datasource_type, r.datasource_id, session)
                user = security_manager.get_user_by_id(r.created_by_fk)
                if not datasource or \
                        security_manager.datasource_access(datasource, user):
                    # datasource does not exist anymore
                    session.delete(r)
            session.commit()
        datasource_type = request.args.get('datasource_type')
        datasource_id = request.args.get('datasource_id')
        created_by_username = request.args.get('created_by')
        role_to_grant = request.args.get('role_to_grant')
        role_to_extend = request.args.get('role_to_extend')

        session = db.session
        datasource = ConnectorRegistry.get_datasource(
            datasource_type, datasource_id, session)

        if not datasource:
            flash(DATASOURCE_MISSING_ERR, 'alert')
            return json_error_response(DATASOURCE_MISSING_ERR)

        requested_by = security_manager.find_user(username=created_by_username)
        if not requested_by:
            flash(USER_MISSING_ERR, 'alert')
            return json_error_response(USER_MISSING_ERR)

        requests = (
            session.query(DAR)
                .filter(
                DAR.datasource_id == datasource_id,
                DAR.datasource_type == datasource_type,
                DAR.created_by_fk == requested_by.id)
                .all()
        )

        if not requests:
            flash(ACCESS_REQUEST_MISSING_ERR, 'alert')
            return json_error_response(ACCESS_REQUEST_MISSING_ERR)

        # check if you can approve
        if security_manager.all_datasource_access() or g.user.id == datasource.owner_id:
            # can by done by admin only
            if role_to_grant:
                role = security_manager.find_role(role_to_grant)
                requested_by.roles.append(role)
                msg = __(
                    '%(user)s was granted the role %(role)s that gives access '
                    'to the %(datasource)s',
                    user=requested_by.username,
                    role=role_to_grant,
                    datasource=datasource.full_name)
                utils.notify_user_about_perm_udate(
                    g.user, requested_by, role, datasource,
                    'email/role_granted.txt', app.config)
                flash(msg, 'info')

            if role_to_extend:
                perm_view = security_manager.find_permission_view_menu(
                    'email/datasource_access', datasource.perm)
                role = security_manager.find_role(role_to_extend)
                security_manager.add_permission_role(role, perm_view)
                msg = __('Role %(r)s was extended to provide the access to '
                         'the datasource %(ds)s', r=role_to_extend,
                         ds=datasource.full_name)
                utils.notify_user_about_perm_udate(
                    g.user, requested_by, role, datasource,
                    'email/role_extended.txt', app.config)
                flash(msg, 'info')
            clean_fulfilled_requests(session)
        else:
            flash(__('You have no permission to approve this request'),
                  'danger')
            return redirect('/accessrequestsmodelview/list/')
        for r in requests:
            session.delete(r)
        session.commit()
        return redirect('/accessrequestsmodelview/list/')

    def get_form_data(self, slice_id=None):
        form_data = {}
        post_data = request.form.get('form_data')
        request_args_data = request.args.get('form_data')
        # Supporting POST
        if post_data:
            form_data.update(json.loads(post_data))
        # request params can overwrite post body
        if request_args_data:
            form_data.update(json.loads(request_args_data))

        url_id = request.args.get('r')
        if url_id:
            saved_url = db.session.query(Url).filter_by(id=url_id).first()
            if saved_url:
                url_str = parse.unquote_plus(
                    saved_url.url.split('?')[1][10:], encoding='utf-8', errors=None)
                url_form_data = json.loads(url_str)
                # allow form_date in request override saved url
                url_form_data.update(form_data)
                form_data = url_form_data

        if request.args.get('viz_type'):
            # Converting old URLs
            form_data = cast_form_data(form_data)

        form_data = {
            k: v
            for k, v in form_data.items()
            if k not in FORM_DATA_KEY_BLACKLIST
        }

        # When a slice_id is present, load from DB and override
        # the form_data from the DB with the other form_data provided
        slice_id = form_data.get('slice_id') or slice_id
        slc = None

        if slice_id:
            slc = db.session.query(Slice).filter_by(id=slice_id).first()
            slice_form_data = slc.form_data.copy()
            # allow form_data in request override slice from_data
            slice_form_data.update(form_data)
            form_data = slice_form_data

        return form_data, slc

    def get_viz(self, slice_id=None, form_data=None, datasource_type=None,
                datasource_id=None, force=False, database_id=None, full_tb_name=None):
        if slice_id:
            slc = db.session.query(Slice).filter_by(id=slice_id).one()
            return slc.get_viz()
        else:
            viz_type = form_data.get('viz_type', 'table')
            if database_id and full_tb_name:
                datasource = Dataset.temp_dataset(database_id, full_tb_name)
            else:
                datasource = ConnectorRegistry.get_datasource(
                    datasource_type, datasource_id, db.session)
            if not datasource:
                raise PropertyException('Missing a dataset for slice')
            if not datasource.database:
                raise PropertyException(
                    'Missing connection for dataset: [{}]'.format(datasource))
            viz_obj = viz.viz_types[viz_type](
                datasource, form_data=form_data, force=force,
            )
            return viz_obj

    @expose("/release/<model>/<action>/<id>/", methods=['GET'])
    def release_object(self, model, action, id):
        """model: dashboard, slice, dataset, database, hdfsconnection
           action: online, offline
           """
        cls = str_to_model.get(model)
        obj = db.session.query(cls).filter_by(id=id).first()
        if not obj:
            msg = _("Not found the object: model={model}, id={id}") \
                .format(model=cls.__name__, id=id)
            logging.error(msg)
            return json_response(status=400, message=msg)
        self.check_release_perm(obj.guardian_datasource())

        if action.lower() == 'online':
            if obj.online is True:
                return json_response(message=OBJECT_IS_ONLINE)
            else:
                self.release_relations(obj, model, g.user.id)
                return json_response(message=ONLINE_SUCCESS)
        elif action.lower() == 'offline':
            if obj.online is False:
                return json_response(message=OBJECT_IS_OFFLINE)
            else:
                obj.online = False
                db.session.commit()
                Log.log_offline(obj, model, g.user.id)
                return json_response(message=OFFLINE_SUCCESS)
        else:
            msg = _('Error request url: [{url}]').format(url=request.url)
            raise ErrorRequestException(msg)

    @classmethod
    def release_relations(cls, obj, model, user_id):
        if str(obj.created_by_fk) == str(user_id) and obj.online is False:
            obj.online = True
            db.session.commit()
            Log.log_online(obj, model, user_id)
        if model == 'dashboard':
            for slice in obj.slices:
                cls.release_relations(slice, 'slice', user_id)
        elif model == 'slice':
            if obj.datasource_id and obj.datasource:
                cls.release_relations(obj.datasource, 'dataset', user_id)
            elif obj.database_id:
                database = db.session.query(Database).filter_by(id=obj.database_id).first()
                if database and database.online is False:
                    cls.release_relations(database, 'database', user_id)
        elif model == 'dataset':
            if obj.database:
                cls.release_relations(obj.database, 'database', user_id)
            if obj.hdfs_table and obj.hdfs_table.hdfs_connection:
                cls.release_relations(obj.hdfs_table.hdfs_connection, 'hdfsconnection', user_id)

    @expose("/slice/<slice_id>/")
    def slice(self, slice_id):
        form_data, slc = self.get_form_data(slice_id)
        endpoint = '/superset/explore/?form_data={}'.format(
            parse.quote(json.dumps(form_data)),
        )
        if request.args.get('standalone') == 'true':
            endpoint += '&standalone=true'
        return redirect(endpoint)

    def get_query_string_response(self, viz_obj):
        query = None
        try:
            query_obj = viz_obj.query_obj()
            if query_obj:
                query = viz_obj.datasource.get_query_str(query_obj)
        except Exception as e:
            logging.exception(e)
            return json_error_response(e)

        if query_obj and query_obj['prequeries']:
            query_obj['prequeries'].append(query)
            query = ';\n\n'.join(query_obj['prequeries'])
        if query:
            query += ';'
        else:
            query = 'No query.'

        return Response(
            json.dumps({
                'query': query,
                'language': viz_obj.datasource.query_language,
            }),
            status=200,
            mimetype='application/json')

    def generate_json(self, datasource_type, datasource_id, form_data,
                      csv=False, query=False, force=False):
        # TODO check guardian permission
        try:
            viz_obj = self.get_viz(
                datasource_type=datasource_type,
                datasource_id=datasource_id,
                form_data=form_data,
                force=force,
            )
        except Exception as e:
            logging.exception(e)
            return json_error_response(
                utils.error_msg_from_exception(e),
                stacktrace=traceback.format_exc())

        # if not security_manager.datasource_access(viz_obj.datasource, g.user):
        if not self.check_read_perm(viz_obj.datasource.guardian_datasource(),
                                    raise_if_false=False):
            return json_error_response(
                DATASOURCE_ACCESS_ERR, status=404, link=config.get(
                    'PERMISSION_INSTRUCTIONS_LINK'))

        if csv:
            return CsvResponse(
                viz_obj.get_csv(),
                status=200,
                headers=generate_download_headers('csv'),
                mimetype='application/csv')

        if query:
            return self.get_query_string_response(viz_obj)

        try:
            payload = viz_obj.get_payload()
        except SupersetException2 as se:
            logging.exception(se)
            return json_error_response(utils.error_msg_from_exception(se),
                                       status=se.status)
        except Exception as e:
            logging.exception(e)
            return json_error_response(utils.error_msg_from_exception(e))

        status = 200
        if payload.get('status') == QueryStatus.FAILED\
                or payload.get('error') is not None:
            status = 400

        return json_success(viz_obj.json_dumps(payload), status=status)

    @expose('/slice_json/<slice_id>')
    def slice_json(self, slice_id):
        try:
            form_data, slc = self.get_form_data(slice_id)
            datasource_type = slc.datasource.type
            datasource_id = slc.datasource.id

        except Exception as e:
            return json_error_response(
                utils.error_msg_from_exception(e),
                stacktrace=traceback.format_exc())
        return self.generate_json(datasource_type=datasource_type,
                                  datasource_id=datasource_id,
                                  form_data=form_data)

    @expose('/annotation_json/<layer_id>')
    def annotation_json(self, layer_id):
        form_data = self.get_form_data()[0]
        form_data['layer_id'] = layer_id
        form_data['filters'] = [{'col': 'layer_id', 'op': '==', 'val': layer_id}]
        datasource = AnnotationDatasource()
        viz_obj = viz.viz_types['table'](
            datasource,
            form_data=form_data,
            force=False,
        )
        try:
            payload = viz_obj.get_payload()
        except Exception as e:
            logging.exception(e)
            return json_error_response(utils.error_msg_from_exception(e))
        status = 200
        if payload.get('status') == QueryStatus.FAILED:
            status = 400
        return json_success(viz_obj.json_dumps(payload), status=status)

    @expose("/explore_json/<datasource_type>/<datasource_id>/")
    @expose('/explore_json/', methods=['GET', 'POST'])
    def explore_json(self, datasource_type=None, datasource_id=None):
        """render the chart of slice"""
        try:
            csv = request.args.get('csv') == 'true'
            query = request.args.get('query') == 'true'
            force = request.args.get('force') == 'true'
            form_data = self.get_form_data()[0]
            datasource_id, datasource_type = self.datasource_info(
                datasource_id, datasource_type, form_data)
        except Exception as e:
            logging.exception(e)
            return json_error_response(
                utils.error_msg_from_exception(e),
                stacktrace=traceback.format_exc())
        return self.generate_json(datasource_type=datasource_type,
                                  datasource_id=datasource_id,
                                  form_data=form_data,
                                  csv=csv,
                                  query=query,
                                  force=force)

    @expose('/import_dashboards', methods=['GET', 'POST'])
    def import_dashboards(self):
        """Overrides the dashboards using json instances from the file."""
        f = request.files.get('file')
        if request.method == 'POST' and f:
            current_tt = int(time.time())
            data = json.loads(f.stream.read(), object_hook=utils.decode_dashboards)
            # TODO: import DRUID datasources
            for table in data['datasources']:
                type(table).import_obj(table, import_time=current_tt)
            db.session.commit()
            for dashboard in data['dashboards']:
                Dashboard.import_obj(
                    dashboard, import_time=current_tt)
            db.session.commit()
            return redirect('/dashboardmodelview/list/')
        return self.render_template('superset/import_dashboards.html')

    @expose('/explorev2/<datasource_type>/<datasource_id>/')
    def explorev2(self, datasource_type, datasource_id):
        """Deprecated endpoint, here for backward compatibility of urls"""
        return redirect(url_for(
            'Superset.explore',
            datasource_type=datasource_type,
            datasource_id=datasource_id,
            **request.args))

    @staticmethod
    def datasource_info(datasource_id, datasource_type, form_data):
        """Compatibility layer for handling of datasource info

        datasource_id & datasource_type used to be passed in the URL
        directory, now they should come as part of the form_data,
        This function allows supporting both without duplicating code"""
        datasource = form_data.get('datasource', '')
        if '__' in datasource:
            datasource_id, datasource_type = datasource.split('__')
            # The case where the datasource has been deleted
            datasource_id = None if datasource_id == 'None' else datasource_id

        if not datasource_id:
            raise Exception(
                'The datasource associated with this chart no longer exists')
        datasource_id = int(datasource_id)
        return datasource_id, datasource_type

    @expose("/explore/<datasource_type>/<datasource_id>/")
    @expose('/explore/', methods=['GET', 'POST'])
    def explore(self, datasource_type=None, datasource_id=None):
        """render the parameters of slice"""
        user_id = g.user.get_id() if g.user else None
        form_data, slc = self.get_form_data()

        datasource_id, datasource_type = self.datasource_info(
            datasource_id, datasource_type, form_data)

        error_redirect = '/slice/list/'
        datasource = ConnectorRegistry.get_datasource(
            datasource_type, datasource_id, db.session)

        if not datasource:
            flash(DATASOURCE_MISSING_ERR, 'danger')
            return redirect(error_redirect)

        # if not security_manager.datasource_access(datasource):
        if not self.check_read_perm(datasource.guardian_datasource(),
                                    raise_if_false=False):
            flash(__(get_datasource_access_error_msg(datasource.name)), 'danger')
            return redirect(
                'superset/request_access/?'
                'datasource_type={datasource_type}&'
                'datasource_id={datasource_id}&'
                ''.format(**locals()))

        viz_type = form_data.get('viz_type')
        if not viz_type and datasource.default_endpoint:
            return redirect(datasource.default_endpoint)

        # slice_add_perm = security_manager.can_access('can_add', 'SliceModelView')
        # slice_overwrite_perm = is_owner(slc, g.user)
        # slice_download_perm = security_manager.can_access(
        #     'can_download', 'SliceModelView')
        slice_add_perm = True
        slice_download_perm = True
        if not slc:
            slice_overwrite_perm = True
        else:
            slice_overwrite_perm = self.check_edit_perm(slc.guardian_datasource(),
                                                        raise_if_false=False)

        form_data['datasource'] = str(datasource_id) + '__' + datasource_type

        # On explore, merge extra filters into the form data
        utils.split_adhoc_filters_into_base_filters(form_data)
        merge_extra_filters(form_data)

        # merge request url params
        if request.method == 'GET':
            merge_request_params(form_data, request.args)

        # handle save or overwrite
        action = request.args.get('action')

        if action == 'overwrite' and not slice_overwrite_perm:
            return json_error_response(
                _('You don\'t have the rights to ') + _('alter this ') + _('chart'),
                status=400)

        if action == 'saveas' and not slice_add_perm:
            return json_error_response(
                _('You don\'t have the rights to ') + _('create a ') + _('chart'),
                status=400)

        if action in ('saveas', 'overwrite'):
            return self.save_or_overwrite_slice(
                request.args,
                slc, slice_add_perm,
                slice_overwrite_perm,
                slice_download_perm,
                datasource_id,
                datasource_type,
                datasource.name)

        standalone = request.args.get('standalone') == 'true'
        bootstrap_data = {
            'can_add': slice_add_perm,
            'can_download': slice_download_perm,
            'can_overwrite': slice_overwrite_perm,
            'datasource': datasource.data,
            'form_data': form_data,
            'datasource_id': datasource_id,
            'datasource_type': datasource_type,
            'slice': slc.data if slc else None,
            'standalone': standalone,
            'user_id': user_id,
            'forced_height': request.args.get('height'),
            'common': self.common_bootsrap_payload(),
        }
        table_name = datasource.table_name \
            if datasource_type == 'table' \
            else datasource.datasource_name
        if slc:
            title = slc.slice_name
        else:
            title = 'Explore - ' + table_name
        return self.render_template(
            'superset/basic.html',
            bootstrap_data=json.dumps(bootstrap_data),
            entry='explore',
            title=title,
            standalone_mode=standalone)

    @expose("/filter/<datasource_type>/<datasource_id>/<column>/")
    def filter(self, datasource_type, datasource_id, column):
        """
        Endpoint to retrieve values for specified column.

        :param datasource_type: Type of datasource e.g. table
        :param datasource_id: Datasource id
        :param column: Column name to retrieve values for
        :return:
        """
        # TODO: Cache endpoint by user, datasource and column
        datasource = ConnectorRegistry.get_datasource(
            datasource_type, datasource_id, db.session)
        if not datasource:
            return json_error_response(DATASOURCE_MISSING_ERR)

        # if not security_manager.datasource_access(datasource):
        if not self.check_read_perm(datasource.guardian_datasource(),
                                    raise_if_false=False):
            return json_error_response(DATASOURCE_ACCESS_ERR)

        payload = json.dumps(
            datasource.values_for_column(
                column,
                config.get('FILTER_SELECT_ROW_LIMIT', 10000),
            ),
            default=utils.json_int_dttm_ser)
        return json_success(payload)

    def check_slice_explore_perm(self, slice_id, dataset_id, database_id, full_tb_name):
        if self.guardian_auth is True:
            if slice_id:
                slice = Slice.get_object(id=slice_id)
                self.check_read_perm(slice.guardian_datasource())
            if database_id and full_tb_name:
                database = Database.get_object(id=database_id)
                self.check_read_perm(database.guardian_datasource())
            else:
                dataset = Dataset.get_object(id=dataset_id)
                self.check_read_perm(dataset.guardian_datasource())
                if dataset.database:
                    self.check_read_perm(dataset.database.guardian_datasource())
                if dataset.hdfs_table and dataset.hdfs_table.hdfs_connection:
                    self.check_read_perm(
                        dataset.hdfs_table.hdfs_connection.guardian_datasource())

    def save_or_overwrite_slice(
            self, args, slc, slice_add_perm, slice_overwrite_perm, slice_download_perm,
            datasource_id, datasource_type, datasource_name):
        """Save or overwrite a slice"""
        slice_name = args.get('slice_name')
        action = args.get('action')
        form_data, _ = self.get_form_data()

        if action in ('saveas'):
            if 'slice_id' in form_data:
                form_data.pop('slice_id')  # don't save old slice_id
            slc = Slice(owners=[g.user] if g.user else [])

        slc.params = json.dumps(form_data)
        slc.datasource_name = datasource_name
        slc.viz_type = form_data['viz_type']
        slc.datasource_type = datasource_type
        slc.datasource_id = datasource_id
        slc.slice_name = slice_name
        # slc.database_id = database_id if database_id else None
        # slc.full_table_name = full_tb_name
        # SliceModelView().check_column_values(slc)

        if action in ('saveas') and slice_add_perm:
            self.save_slice(slc)
        elif action == 'overwrite' and slice_overwrite_perm:
            self.overwrite_slice(slc)

        # Adding slice to a dashboard if requested
        dash = None
        if request.args.get('add_to_dash') == 'existing':
            dash = (
                db.session.query(Dashboard)
                    .filter_by(id=int(request.args.get('save_to_dashboard_id')))
                    .one()
            )

            # check edit dashboard permissions
            # dash_overwrite_perm = check_ownership(dash, raise_if_false=False)
            dash_overwrite_perm = self.check_read_perm(dash.guardian_datasource(),
                                                       raise_if_false=False)
            if not dash_overwrite_perm:
                return json_error_response(
                    _('You don\'t have the rights to ') + _('alter this ') +
                    _('dashboard'),
                    status=400)

            flash(
                'Slice [{}] was added to dashboard [{}]'.format(
                    slc.slice_name,
                    dash.name),
                'info')
        elif request.args.get('add_to_dash') == 'new':
            # check create dashboard permissions
            # dash_add_perm = security_manager.can_access('can_add', 'DashboardModelView')
            # if not dash_add_perm:
            #     return json_error_response(
            #         _('You don\'t have the rights to ') + _('create a ') + _('dashboard'),
            #         status=400)

            dash = Dashboard(
                name=request.args.get('new_dashboard_name'),
                owners=[g.user] if g.user else [])
            flash(
                'Dashboard [{}] just got created and slice [{}] was added '
                'to it'.format(dash.name, slc.slice_name),
                'info')

        if dash and slc not in dash.slices:
            dash.slices.append(slc)
            db.session.commit()
            Log.log_update(dash, 'dashboard', g.user.id)
            dash_view = DashboardModelView()
            dash_view._add(dash)

        response = {
            'can_add': slice_add_perm,
            'can_download': slice_download_perm,
            'can_overwrite': slice_overwrite_perm,
            'form_data': slc.form_data,
            'slice': slc.data,
        }

        if request.args.get('goto_dash') == 'true':
            response.update({'dashboard': dash.url})

        return json_success(json.dumps(response))

    def save_slice(self, slc):
        Slice.check_name(slc.slice_name)
        db.session.expunge_all()
        db.session.add(slc)
        db.session.commit()
        flash(_("Slice [{slice}] has been saved").format(slice=slc.slice_name), "info")
        self.grant_owner_perms(slc.guardian_datasource())
        Number.log_number(g.user.username, Slice.model_type)
        Log.log_add(slc, Slice.model_type, g.user.id)

    def overwrite_slice(self, slc):
        db.session.expunge_all()
        db.session.merge(slc)
        db.session.commit()
        flash(_("Slice [{slice}] has been overwritten").format(slice=slc.slice_name),
              "info")
        Log.log_update(slc, 'slice', g.user.id)

    @expose('/checkbox/<model_view>/<id_>/<attr>/<value>', methods=['GET'])
    def checkbox(self, model_view, id_, attr, value):
        """endpoint for checking/unchecking any boolean in a sqla model"""
        modelview_to_model = {
            'TableColumnInlineView':
                ConnectorRegistry.sources['table'].column_class,
            'DruidColumnInlineView':
                ConnectorRegistry.sources['druid'].column_class,
        }
        model = modelview_to_model[model_view]
        col = db.session.query(model).filter_by(id=id_).first()
        checked = value == 'true'
        if col:
            setattr(col, attr, checked)
            if checked:
                metrics = col.get_metrics().values()
                col.datasource.add_missing_metrics(metrics)
            db.session.commit()
        return json_success('OK')

    @expose("/all_tables/<db_id>/")
    def all_tables(self, db_id):
        """Endpoint that returns all tables and views from the database"""
        database = db.session.query(Database).filter_by(id=db_id).one()
        all_tables = []
        all_views = []
        schemas = database.all_schema_names()
        for schema in schemas:
            all_tables.extend(database.all_table_names(schema=schema))
            all_views.extend(database.all_view_names(schema=schema))
        if not schemas:
            all_tables.extend(database.all_table_names())
            all_views.extend(database.all_view_names())
        return json_response(data={"tables": all_tables, "views": all_views})

    @expose('/schemas/<db_id>/')
    def schemas(self, db_id):
        db_id = int(db_id)
        database = db.session.query(Database).filter_by(id=db_id).one()
        schemas = database.all_schema_names()
        # schemas = security_manager.schemas_accessible_by_user(database, schemas)
        return Response(
            json.dumps({'schemas': schemas}),
            mimetype='application/json')

    @expose('/tables/<db_id>/<schema>/<substr>/')
    def tables(self, db_id, schema, substr):
        """Endpoint to fetch the list of tables for given database"""
        db_id = int(db_id)
        schema = utils.js_string_to_python(schema)
        substr = utils.js_string_to_python(substr)
        database = db.session.query(Database).filter_by(id=db_id).one()
        # table_names = security_manager.accessible_by_user(
        #     database, database.all_table_names(schema), schema)
        # view_names = security_manager.accessible_by_user(
        #     database, database.all_view_names(schema), schema)
        table_names = database.all_table_names(schema=schema)
        view_names = database.all_view_names(schema=schema)

        if substr:
            table_names = [tn for tn in table_names if substr in tn]
            view_names = [vn for vn in view_names if substr in vn]

        max_items = config.get('MAX_TABLE_NAMES') or len(table_names)
        total_items = len(table_names) + len(view_names)
        max_tables = len(table_names)
        max_views = len(view_names)
        if total_items and substr:
            max_tables = max_items * len(table_names) // total_items
            max_views = max_items * len(view_names) // total_items

        table_options = [{'value': tn, 'label': tn}
                         for tn in table_names[:max_tables]]
        table_options.extend([{'value': vn, 'label': '[view] {}'.format(vn)}
                              for vn in view_names[:max_views]])
        payload = {
            'tableLength': len(table_names) + len(view_names),
            'options': table_options,
        }
        return json_success(json.dumps(payload))

    @expose("/copy_dash/<dashboard_id>/", methods=['GET', 'POST'])
    def copy_dash(self, dashboard_id):
        """Copy dashboard"""
        session = db.session()
        data = json.loads(request.form.get('data'))
        dash = Dashboard()
        original_dash = (
            session
                .query(Dashboard)
                .filter_by(id=dashboard_id).first())

        dash.owners = [g.user] if g.user else []
        dash.name = data['dashboard_title']
        if data['duplicate_slices']:
            # Duplicating slices as well, mapping old ids to new ones
            old_to_new_sliceids = {}
            for slc in original_dash.slices:
                new_slice = slc.clone()
                new_slice.owners = [g.user] if g.user else []
                session.add(new_slice)
                session.flush()
                new_slice.dashboards.append(dash)
                old_to_new_sliceids['{}'.format(slc.id)] = \
                    '{}'.format(new_slice.id)
            for d in data['positions']:
                d['slice_id'] = old_to_new_sliceids[d['slice_id']]
        else:
            dash.slices = original_dash.slices
        dash.params = original_dash.params

        self._set_dash_metadata(dash, data)
        session.add(dash)
        session.commit()
        dash_json = dash.json_data
        self.grant_owner_perms(dash.guardian_datasource())
        Number.log_number(g.user.username, Dashboard.model_type)
        Log.log_add(dash, Dashboard.model_type, g.user.id)
        return json_success(dash_json)

    @expose("/save_dash/<dashboard_id>/", methods=['GET', 'POST'])
    def save_dash(self, dashboard_id):
        """Save a dashboard's metadata"""
        session = db.session()
        dash = session.query(Dashboard).filter_by(id=dashboard_id).first()
        self.check_edit_perm(dash.guardian_datasource())
        data = json.loads(request.form.get('data'))
        self._set_dash_metadata(dash, data)
        dash.need_capture = True
        session.merge(dash)
        session.commit()
        Log.log_update(dash, 'dashboard', g.user.id)
        return 'SUCCESS'

    @staticmethod
    def _set_dash_metadata(dashboard, data):
        positions = data['positions']
        slice_ids = [int(d['slice_id']) for d in positions]
        dashboard.slices = [o for o in dashboard.slices if o.id in slice_ids]
        positions = sorted(data['positions'], key=lambda x: int(x['slice_id']))
        dashboard.position_json = json.dumps(positions, indent=4, sort_keys=True)
        md = dashboard.params_dict
        dashboard.css = data['css']
        dashboard.name = data['dashboard_title']

        if 'filter_immune_slices' not in md:
            md['filter_immune_slices'] = []
        if 'timed_refresh_immune_slices' not in md:
            md['timed_refresh_immune_slices'] = []
        if 'filter_immune_slice_fields' not in md:
            md['filter_immune_slice_fields'] = {}
        md['expanded_slices'] = data['expanded_slices']
        md['default_filters'] = data.get('default_filters', '')
        dashboard.json_metadata = json.dumps(md, indent=4)

    @expose("/add_slices/<dashboard_id>/", methods=['POST'])
    def add_slices(self, dashboard_id):
        """Add and save slices to a dashboard"""
        data = json.loads(request.form.get('data'))
        session = db.session()
        dash = session.query(Dashboard).filter_by(id=dashboard_id).first()
        self.check_edit_perm(dash.guardian_datasource())
        new_slices = session.query(Slice).filter(Slice.id.in_(data['slice_ids']))
        dash.slices += new_slices
        dash.need_capture = True
        session.merge(dash)
        session.commit()
        session.close()
        return 'SLICES ADDED'

    @connection_timeout
    @expose("/testconn/", methods=["POST", "GET"])
    def testconn(self):
        """Tests a sqla connection"""
        args = json.loads(str(request.data, encoding='utf-8'))
        uri = args.get('sqlalchemy_uri')
        db_name = args.get('database_name')
        if db_name:
            database = (
                db.session.query(Database).filter_by(database_name=db_name).first()
            )
            if database and uri == database.safe_sqlalchemy_uri():
                uri = database.sqlalchemy_uri_decrypted
        connect_args = eval(args.get('args', {})).get('connect_args', {})
        connect_args = Database.append_args(connect_args)
        engine = create_engine(uri, connect_args=connect_args)
        try:
            tables = engine.table_names()
            return json_response(data=tables)
        except Exception as e:
            raise DatabaseException(str(e))

    @expose('/recent_activity/<user_id>/', methods=['GET'])
    def recent_activity(self, user_id):
        """Recent activity (actions) for a given user"""
        if request.args.get('limit'):
            limit = int(request.args.get('limit'))
        else:
            limit = 1000

        qry = (
            db.session.query(Log, Dashboard, Slice)
            .outerjoin(Dashboard, Dashboard.id == Log.dashboard_id)
            .outerjoin(Slice, Slice.id == Log.slice_id)
            .filter(
                sqla.and_(
                    ~Log.action.in_(('queries', 'shortner', 'sql_json')),
                    Log.user_id == user_id,
                ),
            )
            .order_by(Log.dttm.desc())
            .limit(limit)
        )
        payload = []
        for log in qry.all():
            item_url = None
            item_title = None
            if log.Dashboard:
                item_url = log.Dashboard.url
                item_title = log.Dashboard.name
            elif log.Slice:
                item_url = log.Slice.slice_url
                item_title = log.Slice.slice_name

            payload.append({
                'action': log.Log.action,
                'item_url': item_url,
                'item_title': item_title,
                'time': log.Log.dttm,
            })
        return json_success(
            json.dumps(payload, default=utils.json_int_dttm_ser))

    @expose('/csrf_token/', methods=['GET'])
    def csrf_token(self):
        return Response(
            self.render_template('superset/csrf_token.json'),
            mimetype='text/json',
        )

    @expose('/fave_dashboards_by_username/<username>/', methods=['GET'])
    def fave_dashboards_by_username(self, username):
        """This lets us use a user's username to pull favourite dashboards"""
        user = security_manager.find_user(username=username)
        return self.fave_dashboards(user.get_id())

    @expose('/fave_dashboards/<user_id>/', methods=['GET'])
    def fave_dashboards(self, user_id):
        qry = (
            db.session.query(Dashboard, FavStar.dttm)
            .join(
                FavStar,
                sqla.and_(
                    FavStar.user_id == int(user_id),
                    FavStar.class_name == 'Dashboard',
                    Dashboard.id == FavStar.obj_id,
                ),
            )
            .order_by(FavStar.dttm.desc(),
            )
        )
        payload = []
        for o in qry.all():
            d = {
                'id': o.Dashboard.id,
                'dashboard': o.Dashboard.dashboard_link(),
                'title': o.Dashboard.name,
                'url': o.Dashboard.url,
                'dttm': o.dttm,
            }
            if o.Dashboard.created_by:
                user = o.Dashboard.created_by
                d['creator'] = str(user)
                d['creator_url'] = '/superset/profile/{}/'.format(
                    user.username)
            payload.append(d)
        return json_success(
            json.dumps(payload, default=utils.json_int_dttm_ser))

    @expose('/created_dashboards/<user_id>/', methods=['GET'])
    def created_dashboards(self, user_id):
        Dash = Dashboard  # noqa
        qry = (
            db.session.query(Dash)
            .filter(
                sqla.or_(
                    Dash.created_by_fk == user_id,
                    Dash.changed_by_fk == user_id,
                ),
            )
            .order_by(Dash.changed_on.desc(),
            )
        )
        payload = [{
            'id': o.id,
            'dashboard': o.dashboard_link(),
            'title': o.name,
            'url': o.url,
            'dttm': o.changed_on,
        } for o in qry.all()]
        return json_success(
            json.dumps(payload, default=utils.json_int_dttm_ser))

    @expose('/user_slices', methods=['GET'])
    @expose('/user_slices/<user_id>/', methods=['GET'])
    def user_slices(self, user_id=None):
        """List of slices a user created, or faved"""
        if not user_id:
            user_id = g.user.id
        qry = (
            db.session.query(Slice,FavStar.dttm)
            .join(
                FavStar,
                sqla.and_(
                    FavStar.user_id == int(user_id),
                    FavStar.class_name == 'slice',
                    Slice.id == FavStar.obj_id,
                ),
                isouter=True
            )
            .filter(
                sqla.or_(
                    Slice.created_by_fk == user_id,
                    Slice.changed_by_fk == user_id,
                    FavStar.user_id == user_id,
                ),
            )
            .order_by(Slice.slice_name.asc())
        )
        payload = [{
            'id': o.Slice.id,
            'title': o.Slice.slice_name,
            'url': o.Slice.slice_url,
            'data': o.Slice.form_data,
            'dttm': o.dttm if o.dttm else o.Slice.changed_on,
            'viz_type': o.Slice.viz_type,
        } for o in qry.all()]
        return json_success(
            json.dumps(payload, default=utils.json_int_dttm_ser))

    @expose('/created_slices', methods=['GET'])
    @expose('/created_slices/<user_id>/', methods=['GET'])
    def created_slices(self, user_id=None):
        """List of slices created by this user"""
        if not user_id:
            user_id = g.user.id
        qry = (
            db.session.query(Slice)
            .filter(
                sqla.or_(
                    Slice.created_by_fk == user_id,
                    Slice.changed_by_fk == user_id,
                ),
            )
            .order_by(Slice.changed_on.desc())
        )
        payload = [{
            'id': o.id,
            'title': o.slice_name,
            'url': o.slice_url,
            'dttm': o.changed_on,
            'viz_type': o.viz_type,
        } for o in qry.all()]
        return json_success(
            json.dumps(payload, default=utils.json_int_dttm_ser))

    @expose('/fave_slices', methods=['GET'])
    @expose('/fave_slices/<user_id>/', methods=['GET'])
    def fave_slices(self, user_id=None):
        """Favorite slices for a user"""
        if not user_id:
            user_id = g.user.id
        qry = (
            db.session.query(Slice, FavStar.dttm)
            .join(
                FavStar,
                sqla.and_(
                    FavStar.user_id == int(user_id),
                    FavStar.class_name == 'slice',
                    Slice.id == FavStar.obj_id,
                ),
            )
            .order_by(FavStar.dttm.desc(),
            )
        )
        payload = []
        for o in qry.all():
            d = {
                'id': o.Slice.id,
                'title': o.Slice.slice_name,
                'url': o.Slice.slice_url,
                'dttm': o.dttm,
                'viz_type': o.Slice.viz_type,
            }
            if o.Slice.created_by:
                user = o.Slice.created_by
                d['creator'] = str(user)
                d['creator_url'] = '/superset/profile/{}/'.format(
                    user.username)
            payload.append(d)
        return json_success(
            json.dumps(payload, default=utils.json_int_dttm_ser))

    @expose('/warm_up_cache/', methods=['GET'])
    def warm_up_cache(self):
        """Warms up the cache for the slice or table.

        Note for slices a force refresh occurs.
        """
        slices = None
        session = db.session()
        slice_id = request.args.get('slice_id')
        table_name = request.args.get('table_name')
        db_name = request.args.get('db_name')

        if not slice_id and not (table_name and db_name):
            return json_error_response(__(
                'Malformed request. slice_id or table_name and db_name '
                'arguments are expected'), status=400)
        if slice_id:
            slices = session.query(Slice).filter_by(id=slice_id).all()
            if not slices:
                return json_error_response(__(
                    'Chart %(id)s not found', id=slice_id), status=404)
        elif table_name and db_name:
            SqlaTable = ConnectorRegistry.sources['table']
            table = (
                session.query(SqlaTable)
                .join(Database)
                .filter(
                    Database.database_name == db_name or
                    SqlaTable.table_name == table_name
                )
            ).first()
            if not table:
                return json_error_response(__(
                    "Table %(t)s wasn't found in the database %(d)s",
                    t=table_name, s=db_name), status=404)
            slices = session.query(Slice).filter_by(
                datasource_id=table.id,
                datasource_type=table.type).all()

        for slc in slices:
            try:
                obj = slc.get_viz(force=True)
                obj.get_json()
            except Exception as e:
                return json_error_response(utils.error_msg_from_exception(e))
        return json_success(json.dumps(
            [{'slice_id': slc.id, 'slice_name': slc.slice_name}
             for slc in slices]))

    @expose("/favstar/<class_name>/<obj_id>/<action>/")
    def favstar(self, class_name, obj_id, action):
        """Toggle favorite stars on Slices and Dashboard"""
        session = db.session()
        count = 0
        favs = (
            session.query(FavStar)
                .filter_by(class_name=class_name, obj_id=obj_id, user_id=g.user.get_id())
                .all()
        )
        # get obj name to make log readable
        obj = (
            session.query(str_to_model[class_name.lower()])
                .filter_by(id=obj_id).one()
        )

        if action == 'select':
            if not favs:
                session.add(
                    FavStar(
                        class_name=class_name,
                        obj_id=obj_id,
                        user_id=g.user.get_id(),
                        dttm=datetime.now()
                    )
                )
            count = 1
            Log.log('like', obj, class_name.lower(), g.user.id)
        elif action == 'unselect':
            for fav in favs:
                session.delete(fav)
            Log.log('dislike', obj, class_name.lower(), g.user.id)
        else:
            count = len(favs)
        session.commit()
        return json_success(json.dumps({'count': count}))

    @expose('/if_online/<class_name>/<obj_id>/')
    def if_online(self, class_name, obj_id):
        try:
            model = str_to_model.get(class_name.lower())
            if hasattr(model, 'online'):
                obj = db.session.query(model).filter_by(id=obj_id).first()
                return json_response(data={'online': obj.online})
            else:
                return json_response(data={'online': False})
        except Exception as e:
            return json_response(message=utils.error_msg_from_exception(e),
                                 status=500)

    @expose("/dashboard/<dashboard_id>/")
    def dashboard(self, dashboard_id):
        """Server side rendering for a dashboard"""
        self.update_redirect()
        session = db.session()
        dash = session.query(Dashboard).filter_by(id=int(dashboard_id)).one()
        datasources = set()
        for slc in dash.slices:
            datasource = slc.datasource
            if datasource:
                datasources.add(datasource)

        # if config.get('ENABLE_ACCESS_REQUEST'):
        #     for datasource in datasources:
        #         if datasource and not security_manager.datasource_access(datasource):
        #             flash(
        #                 __(get_datasource_access_error_msg(datasource.name)),
        #                 'danger')
        #             return redirect(
        #                 'superset/request_access/?'
        #                 'dashboard_id={dash.id}&'.format(**locals()))

        # Hack to log the dashboard_id properly, even when getting a slug
        def dashboard(**kwargs):  # noqa
            pass
        dashboard(dashboard_id=dash.id)

        # dash_edit_perm = check_ownership(dash, raise_if_false=False) and \
        #                  security_manager.can_access('can_save_dash', 'Superset')
        # dash_save_perm = security_manager.can_access('can_save_dash', 'Superset')
        # superset_can_explore = security_manager.can_access('can_explore', 'Superset')
        # slice_can_edit = security_manager.can_access('can_edit', 'SliceModelView')
        dash_edit_perm = self.check_edit_perm(
            dash.guardian_datasource(), raise_if_false=False)
        dash_save_perm = True
        superset_can_explore = True
        slice_can_edit = True

        standalone_mode = request.args.get('standalone') == 'true'

        dashboard_data = dash.data
        dashboard_data.update({
            'standalone_mode': standalone_mode,
            'dash_save_perm': dash_save_perm,
            'dash_edit_perm': dash_edit_perm,
            'superset_can_explore': superset_can_explore,
            'slice_can_edit': slice_can_edit,
        })

        bootstrap_data = {
            'user_id': g.user.get_id(),
            'dashboard_data': dashboard_data,
            'datasources': {ds.uid: ds.data for ds in datasources},
            'common': self.common_bootsrap_payload(),
            'editMode': request.args.get('edit') == 'true',
        }

        if request.args.get('json') == 'true':
            return json_success(json.dumps(bootstrap_data))

        return self.render_template(
            'superset/dashboard.html',
            entry='dashboard',
            standalone_mode=standalone_mode,
            title=dash.name,
            bootstrap_data=json.dumps(bootstrap_data),
        )

    @expose('/log/', methods=['POST'])
    def log(self):
        return Response(status=200)

    @expose('/sync_druid/', methods=['POST'])
    def sync_druid_source(self):
        """Syncs the druid datasource in main db with the provided config.

        The endpoint takes 3 arguments:
            user - user name to perform the operation as
            cluster - name of the druid cluster
            config - configuration stored in json that contains:
                name: druid datasource name
                dimensions: list of the dimensions, they become druid columns
                    with the type STRING
                metrics_spec: list of metrics (dictionary). Metric consists of
                    2 attributes: type and name. Type can be count,
                    etc. `count` type is stored internally as longSum
                    other fields will be ignored.

            Example: {
                'name': 'test_click',
                'metrics_spec': [{'type': 'count', 'name': 'count'}],
                'dimensions': ['affiliate_id', 'campaign', 'first_seen']
            }
        """
        payload = request.get_json(force=True)
        druid_config = payload['config']
        user_name = payload['user']
        cluster_name = payload['cluster']

        user = security_manager.find_user(username=user_name)
        DruidDatasource = ConnectorRegistry.sources['druid']
        DruidCluster = DruidDatasource.cluster_class
        if not user:
            err_msg = __("Can't find User '%(name)s', please ask your admin "
                         'to create one.', name=user_name)
            logging.error(err_msg)
            return json_error_response(err_msg)
        cluster = db.session.query(DruidCluster).filter_by(
            cluster_name=cluster_name).first()
        if not cluster:
            err_msg = __("Can't find DruidCluster with cluster_name = "
                         "'%(name)s'", name=cluster_name)
            logging.error(err_msg)
            return json_error_response(err_msg)
        try:
            DruidDatasource.sync_to_db_from_config(
                druid_config, user, cluster)
        except Exception as e:
            logging.exception(utils.error_msg_from_exception(e))
            return json_error_response(utils.error_msg_from_exception(e))
        return Response(status=201)

    @expose("/sqllab_viz/", methods=['POST'])
    def sqllab_viz(self):
        Dataset = ConnectorRegistry.sources['table']
        data = json.loads(request.form.get('data'))
        table_name = data.get('datasourceName')
        template_params = data.get('templateParams')
        table = (
            db.session.query(Dataset)
                .filter_by(table_name=table_name)
                .first()
        )
        if not table:
            table = Dataset(table_name=table_name)
        table.database_id = data.get('dbId')
        table.schema = data.get('schema')
        table.template_params = data.get('templateParams')
        table.is_sqllab_view = True
        q = SupersetQuery(data.get('sql'))
        table.sql = q.stripped()
        db.session.add(table)
        # TODO
        # self.grant_owner_perms(table.guardian_datasource())
        # Number.log_number(g.user.username, Dataset.model_type)
        # Log.log_add(table, Dataset.model_type, g.user.id)

        cols = []
        dims = []
        metrics = []
        for column_name, config in data.get('columns').items():
            is_dim = config.get('is_dim', False)
            SqlaTable = ConnectorRegistry.sources['table']
            TableColumn = SqlaTable.column_class
            SqlMetric = SqlaTable.metric_class
            col = TableColumn(
                column_name=column_name,
                filterable=is_dim,
                groupby=is_dim,
                is_dttm=config.get('is_date', False),
                type=config.get('type', False),
            )
            cols.append(col)
            if is_dim:
                dims.append(col)
            agg = config.get('agg')
            if agg:
                if agg == 'count_distinct':
                    metrics.append(SqlMetric(
                        metric_name='{agg}__{column_name}'.format(**locals()),
                        expression='COUNT(DISTINCT {column_name})'
                            .format(**locals()),
                            ))
                else:
                    metrics.append(SqlMetric(
                        metric_name='{agg}__{column_name}'.format(**locals()),
                        expression='{agg}({column_name})'.format(**locals()),
                    ))
        if not metrics:
            metrics.append(SqlMetric(
                metric_name='count'.format(**locals()),
                expression='count(*)'.format(**locals()),
            ))
        table.ref_columns = cols
        table.ref_metrics = metrics
        db.session.commit()
        return self.json_response(json.dumps({ 'table_id': table.id,}))

    @expose("/table/<database_id>/<table_name>/<schema>/")
    def table(self, database_id, table_name, schema):
        schema = utils.js_string_to_python(schema)
        mydb = db.session.query(Database).filter_by(id=database_id).one()
        payload_columns = []

        try:
            columns = mydb.get_columns(table_name, schema)
            if mydb.backend.lower() == 'inceptor':
                indexes, primary_key, foreign_keys = [], [], []
            else:
                indexes = mydb.get_indexes(table_name, schema)
                primary_key = mydb.get_pk_constraint(table_name, schema)
                foreign_keys = mydb.get_foreign_keys(table_name, schema)
        except Exception as e:
            return json_error_response(utils.error_msg_from_exception(e))
        keys = []
        if primary_key and primary_key.get('constrained_columns'):
            primary_key['column_names'] = primary_key.pop('constrained_columns')
            primary_key['type'] = 'pk'
            keys += [primary_key]
        for fk in foreign_keys:
            fk['column_names'] = fk.pop('constrained_columns')
            fk['type'] = 'fk'
        keys += foreign_keys
        for idx in indexes:
            idx['type'] = 'index'
        keys += indexes

        for col in columns:
            dtype = ''
            try:
                dtype = '{}'.format(col['type'])
            except Exception:
                pass
            payload_columns.append({
                'name': col['name'],
                'type': dtype.split('(')[0] if '(' in dtype else dtype,
                'longType': dtype,
                'keys': [
                    k for k in keys
                    if col['name'] in k.get('column_names')
                ],
            })
        tbl = {
            'name': table_name,
            'columns': payload_columns,
            'selectStar': mydb.select_star(
                table_name, schema=schema, show_cols=True, indent=True,
                cols=columns, latest_partition=False),
            'primaryKey': primary_key,
            'foreignKeys': foreign_keys,
            'indexes': keys,
        }
        return json_success(json.dumps(tbl))

    @expose('/extra_table_metadata/<database_id>/<table_name>/<schema>/')
    def extra_table_metadata(self, database_id, table_name, schema):
        schema = utils.js_string_to_python(schema)
        mydb = db.session.query(Database).filter_by(id=database_id).one()
        payload = mydb.db_engine_spec.extra_table_metadata(
            mydb, table_name, schema)
        return json_success(json.dumps(payload))

    @expose('/select_star/<database_id>/<table_name>/')
    def select_star(self, database_id, table_name):
        mydb = db.session.query(Database).filter_by(id=database_id).first()
        return self.render_template(
            'superset/ajah.html',
            content=mydb.select_star(table_name, show_cols=True),
        )

    @expose("/theme/")
    def theme(self):
        return self.render_template('superset/theme.html')

    @expose('/cached_key/<key>/')
    def cached_key(self, key):
        """Returns a key from the cache"""
        resp = cache.get(key)
        if resp:
            return resp
        return 'nope'

    @expose('/cache_key_exist/<key>/')
    def cache_key_exist(self, key):
        """Returns if a key from cache exist"""
        key_exist = True if cache.get(key) else False
        status = 200 if key_exist else 404
        return json_success(json.dumps({'key_exist': key_exist}), status=status)

    @expose("/results/<key>/")
    def results(self, key):
        """Serves a key off of the results backend"""
        if not results_backend:
            return json_error_response("Results backend isn't configured")

        blob = results_backend.get(key)
        if not blob:
            return json_error_response(
                'Data could not be retrieved. '
                'You may want to re-run the query.',
                status=410,
            )

        query = db.session.query(Query).filter_by(results_key=key).one()
        rejected_tables = security_manager.rejected_datasources(
            query.sql, query.database, query.schema)
        if rejected_tables:
            return json_error_response(get_datasource_access_error_msg(
                '{}'.format(rejected_tables)))

        payload = utils.zlib_decompress_to_string(blob)
        display_limit = app.config.get('DISPLAY_SQL_MAX_ROW', None)
        payload_json = {}
        if display_limit:
            payload_json = json.loads(payload)
            payload_json['data'] = payload_json['data'][:display_limit]
        return json_success(
            json.dumps(
                payload_json, default=utils.json_iso_dttm_ser, ignore_nan=True))

    @expose('/stop_query/', methods=['POST'])
    def stop_query(self):
        client_id = request.form.get('client_id')
        try:
            query = (
                db.session.query(Query).filter_by(client_id=client_id).one()
            )
            query.status = utils.QueryStatus.STOPPED
            db.session.commit()
        except Exception:
            pass
        return self.json_response('OK')

    @expose("/sql_json/", methods=['POST', 'GET'])
    def sql_json(self):
        """Runs arbitrary sql and returns and json"""
        async = request.form.get('runAsync') == 'true'
        sql = request.form.get('sql')
        database_id = request.form.get('database_id')
        schema = request.form.get('schema') or None
        template_params = json.loads(
            request.form.get('templateParams') or '{}')

        session = db.session()
        mydb = session.query(Database).filter_by(id=database_id).first()

        if not mydb:
            json_error_response(
                'Database with id {} is missing.'.format(database_id))

        # rejected_tables = security_manager.rejected_datasources(sql, mydb, schema)
        # if rejected_tables:
        #     return json_error_response(get_datasource_access_error_msg(
        #         '{}'.format(rejected_tables)))
        session.commit()

        select_as_cta = request.form.get('select_as_cta') == 'true'
        tmp_table_name = request.form.get('tmp_table_name')
        if select_as_cta and mydb.force_ctas_schema:
            tmp_table_name = '{}.{}'.format(
                mydb.force_ctas_schema,
                tmp_table_name,
            )

        query = Query(
            database_id=int(database_id),
            limit=mydb.db_engine_spec.get_limit_from_sql(sql),
            sql=sql,
            schema=schema,
            select_as_cta=request.form.get('select_as_cta') == 'true',
            start_time=utils.now_as_float(),
            tab_name=request.form.get('tab'),
            status=QueryStatus.PENDING if async else QueryStatus.RUNNING,
            sql_editor_id=request.form.get('sql_editor_id'),
            tmp_table_name=tmp_table_name,
            user_id=int(g.user.get_id()),
            client_id=request.form.get('client_id'),
        )
        session.add(query)
        session.flush()
        query_id = query.id
        session.commit()  # shouldn't be necessary
        if not query_id:
            raise Exception(_('Query record was not created as expected.'))
        logging.info('Triggering query_id: {}'.format(query_id))

        try:
            template_processor = get_template_processor(
                database=query.database, query=query)
            rendered_query = template_processor.process_template(
                query.sql,
                **template_params)
        except Exception as e:
            return json_error_response(
                'Template rendering failed: {}'.format(utils.error_msg_from_exception(e)))

        # Async request.
        if async:
            logging.info('Running query on a Celery worker')
            # Ignore the celery future object and the request may time out.
            try:
                sql_lab.get_sql_results.delay(
                    query_id,
                    rendered_query,
                    return_results=False,
                    store_results=not query.select_as_cta,
                    user_name=g.user.username)
            except Exception as e:
                logging.exception(e)
                msg = (
                    'Failed to start remote query on a worker. '
                    'Tell your administrator to verify the availability of '
                    'the message queue.'
                )
                query.status = QueryStatus.FAILED
                query.error_message = msg
                session.commit()
                return json_error_response('{}'.format(msg))

            resp = json_success(json.dumps(
                {'query': query.to_dict()}, default=utils.json_int_dttm_ser,
                ignore_nan=True), status=202)
            session.commit()
            return resp

        # Sync request.
        try:
            timeout = config.get('SQLLAB_TIMEOUT')
            timeout_msg = (
                'The query exceeded the {timeout} seconds '
                'timeout.').format(**locals())
            with utils.timeout(seconds=timeout,
                               error_message=timeout_msg):
                # pylint: disable=no-value-for-parameter
                data = sql_lab.get_sql_results(
                    query_id,
                    rendered_query,
                    return_results=True)
            payload = json.dumps(
                data,
                default=utils.pessimistic_json_iso_dttm_ser,
                ignore_nan=True,
                encoding=None,
            )
        except Exception as e:
            logging.exception(e)
            return json_error_response('{}'.format(e))
        if data.get('status') == QueryStatus.FAILED:
            return json_error_response(payload=data)
        return json_success(payload)

    @expose("/csv/<client_id>/")
    def csv(self, client_id):
        """Download the query results as csv.
        For inceptor, superset will create a temp table stored as csv file.
        If the size of results is too large, superset will retain the data files
        in HDFS folder for a period of time, and then drop the temp table.
        """
        with_header = request.args.get('header') == 'true'
        session = db.session()
        query = session.query(Query).filter_by(client_id=client_id).one()
        sql = query.select_sql or query.sql
        database = query.database
        session.close()

        filename = quote('{}.csv'.format(query.name), encoding="utf-8")
        blob = None
        if results_backend and query.results_key:
            logging.info(
                'Fetching CSV from results backend '
                '[{}]'.format(query.results_key))
            blob = results_backend.get(query.results_key)
        if blob:
            logging.info('Decompressing')
            json_payload = utils.zlib_decompress_to_string(blob)
            obj = json.loads(json_payload)
            columns = [c['name'] for c in obj['columns']]
            df = pd.DataFrame.from_records(obj['data'], columns=columns)
            logging.info('Using pandas to convert to CSV')
            csv = df.to_csv(index=False, **config.get('CSV_EXPORT'))
        else:
            logging.info('Running a query to turn into CSV')
            stored_in_hdfs = True if database.is_inceptor else False
            if stored_in_hdfs:
                # Beacuse of JVM attached threads, can't get super user 'superset' 's token
                # by Guardian api, thus can't create a APScheduler thread to drop old
                # temp tables at backend
                sql_lab.drop_inceptor_temp_table(g.user.username)

                engine = database.get_sqla_engine(schema=query.schema, use_pool=False)
                table_name, hdfs_path = sql_lab.store_sql_results_to_hdfs(sql, engine)

                client = HDFSBrowser.get_client()
                data = HDFSBrowser.read_folder(client, hdfs_path)
                if data is None:  # File is too big
                    # return json_response('Data size is huge. You can go to HDFS [{}] to '
                    #                      'download it'.format(hdfs_path))
                    return redirect('/hdfs/?current_path={}'.format(hdfs_path))

                if with_header is True:
                    columns = database.get_columns(table_name, schema=query.schema)
                    names = [c['name'] for c in columns]
                    names = ['"{}"'.format(n) for n in names]
                    name_str = ','.join(names)
                    tmp_barray = bytearray()
                    tmp_barray.extend(name_str.encode('utf8'))
                    tmp_barray.extend(b'\t\n')
                    data[0:0] = tmp_barray

                sql = 'DROP TABLE IF EXISTS {}'.format(table_name)
                logging.info(sql)
                engine.execute(sql)

                data = HDFSBrowser.gzip_compress(data)
                filename = '{}.gz'.format(filename)

                response = Response(bytes(data), content_type='application/octet-stream')
                response.headers['Content-Disposition'] = \
                    "attachment; filename={}".format(filename)
                return response
            else:
                df = database.get_df(sql, query.schema)
                # TODO(bkyryliuk): add compression=gzip for big files.
                csv = df.to_csv(index=False, header=with_header, encoding='utf-8')

        response = Response(csv, mimetype='text/csv')
        response.headers['Content-Disposition'] = (
            'attachment; filename={}'.format(filename))
        logging.info('Ready to return response')
        return response

    @expose('/fetch_datasource_metadata')
    def fetch_datasource_metadata(self):
        datasource_id, datasource_type = (
            request.args.get('datasourceKey').split('__'))
        datasource = ConnectorRegistry.get_datasource(
            datasource_type, datasource_id, db.session)
        # Check if datasource exists
        if not datasource:
            return json_error_response(DATASOURCE_MISSING_ERR)

        # Check permission for datasource
        if not security_manager.datasource_access(datasource):
            return json_error_response(DATASOURCE_ACCESS_ERR)
        return json_success(json.dumps(datasource.data))

    @expose("/queries/<last_updated_ms>/")
    def queries(self, last_updated_ms):
        """Get the updated queries."""
        stats_logger.incr('queries')
        if not g.user.get_id():
            return json_error_response(
                'Please login to access the queries.', status=403)

        # Unix time, milliseconds.
        last_updated_ms_int = int(float(last_updated_ms)) if last_updated_ms else 0
        # UTC date time, same that is stored in the DB.
        last_updated_dt = utils.EPOCH + timedelta(seconds=last_updated_ms_int / 1000)

        sql_queries = (
            db.session.query(Query)
                .filter(
                Query.user_id == g.user.get_id(),
                Query.changed_on >= last_updated_dt,
                )
                .all()
        )
        dict_queries = {q.client_id: q.to_dict() for q in sql_queries}
        now = int(round(time.time() * 1000))
        unfinished_states = [
            utils.QueryStatus.PENDING,
            utils.QueryStatus.RUNNING,
        ]

        queries_to_timeout = [
            client_id for client_id, query_dict in dict_queries.items()
            if (
                query_dict['state'] in unfinished_states and (
                    now - query_dict['startDttm'] >
                    config.get('SQLLAB_ASYNC_TIME_LIMIT_SEC') * 1000
                )
            )
        ]

        if queries_to_timeout:
            update(Query).where(
                and_(
                    Query.user_id == g.user.get_id(),
                    Query.client_id in queries_to_timeout,
                    ),
            ).values(state=utils.QueryStatus.TIMED_OUT)

            for client_id in queries_to_timeout:
                dict_queries[client_id]['status'] = utils.QueryStatus.TIMED_OUT

        return json_success(
            json.dumps(dict_queries, default=utils.json_int_dttm_ser))

    @expose("/search_queries/")
    def search_queries(self):
        """Search for queries."""
        query = db.session.query(Query)
        search_user_id = request.args.get('user_id')
        database_id = request.args.get('database_id')
        search_text = request.args.get('search_text')
        status = request.args.get('status')
        # From and To time stamp should be Epoch timestamp in seconds
        from_time = request.args.get('from')
        to_time = request.args.get('to')

        if search_user_id:
            # Filter on db Id
            query = query.filter(Query.user_id == search_user_id)

        if database_id:
            # Filter on db Id
            query = query.filter(Query.database_id == database_id)

        if status:
            # Filter on status
            query = query.filter(Query.status == status)

        if search_text:
            # Filter on search text
            query = query \
                .filter(Query.sql.like('%{}%'.format(search_text)))

        if from_time:
            query = query.filter(Query.start_time > int(from_time))

        if to_time:
            query = query.filter(Query.start_time < int(to_time))

        query_limit = config.get('QUERY_SEARCH_LIMIT', 1000)
        sql_queries = (
            query.order_by(Query.start_time.asc())
                .limit(query_limit)
                .all()
        )

        dict_queries = [q.to_dict() for q in sql_queries]

        return Response(
            json.dumps(dict_queries, default=utils.json_int_dttm_ser),
            status=200,
            mimetype='application/json')

    @app.errorhandler(500)
    def show_traceback(self):
        return render_template(
            'superset/traceback.html',
            error_msg=get_error_msg(),
        ), 500

    @expose('/welcome')
    def welcome(self):
        """Personalized welcome page"""
        if not g.user or not g.user.get_id():
            return redirect(appbuilder.get_url_for_login)

        payload = {
            'user': bootstrap_user_data(),
            'common': self.common_bootsrap_payload(),
        }

        return self.render_template(
            'superset/basic.html',
            entry='welcome',
            title='Superset',
            bootstrap_data=json.dumps(payload, default=utils.json_iso_dttm_ser),
        )

    @expose('/profile/<username>/')
    def profile(self, username):
        """User profile page"""
        if not username and g.user:
            username = g.user.username

        payload = {
            'user': bootstrap_user_data(username, include_perms=True),
            'common': self.common_bootsrap_payload(),
        }

        return self.render_template(
            'superset/basic.html',
            title=username + "'s profile",
            entry='profile',
            bootstrap_data=json.dumps(payload, default=utils.json_iso_dttm_ser),
        )

    @expose("/sqllab/")
    def sqllab(self):
        """SQL Editor"""
        d = {
            'defaultDbId': config.get('SQLLAB_DEFAULT_DBID'),
            'common': self.common_bootsrap_payload(),
        }
        return self.render_template(
            'superset/basic.html',
            entry='sqllab',
            bootstrap_data=json.dumps(d, default=utils.json_iso_dttm_ser),
        )

    @expose('/slice_query/<slice_id>/')
    def slice_query(self, slice_id):
        """
        This method exposes an API endpoint to
        get the database query string for this slice
        """
        viz_obj = self.get_viz(slice_id)
        if not security_manager.datasource_access(viz_obj.datasource):
            return json_error_response(
                DATASOURCE_ACCESS_ERR, status=401, link=config.get(
                    'PERMISSION_INSTRUCTIONS_LINK'))
        return self.get_query_string_response(viz_obj)


@app.route('/health')
def health():
    return 'OK'


@app.route('/healthcheck')
def healthcheck():
    return 'OK'


@app.route('/ping')
def ping():
    return 'OK'


@app.after_request
def apply_caching(response):
    """Applies the configuration's http headers to all responses"""
    for k, v in config.get('HTTP_HEADERS').items():
        response.headers[k] = v
    return response


# ---------------------------------------------------------------------
# Redirecting URL from previous names
class RegexConverter(BaseConverter):
    def __init__(self, url_map, *items):
        super(RegexConverter, self).__init__(url_map)
        self.regex = items[0]


app.url_map.converters['regex'] = RegexConverter

