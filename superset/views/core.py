import json
import logging
import sys
import zlib
from datetime import datetime, timedelta
from flask import g, request, redirect, flash, Response, Markup
from flask_babel import lazy_gettext as _
from flask_appbuilder import expose
from flask_appbuilder.models.sqla.interface import SQLAInterface
from sqlalchemy import create_engine
from urllib.parse import quote

from superset import app, db, sql_lab, results_backend, viz, utils
from superset.timeout_decorator import connection_timeout
from superset.source_registry import SourceRegistry
from superset.sql_parse import SupersetQuery
from superset.message import *
from superset.exception import (
    ParameterException, PropertyException, DatabaseException, ErrorRequestException,
)
from superset.models import (
    Database, Dataset, Slice, Dashboard, TableColumn, SqlMetric, Query, Log,
    FavStar, str_to_model, Number, Url, KeyValue, CssTemplate
)
from .base import (
    BaseSupersetView, SupersetModelView, DeleteMixin, PermissionManagement,
    catch_exception, json_response
)
from .base import json_error_response
from .hdfs import HDFSBrowser
from .dashboard import DashboardModelView
from .slice import SliceModelView


config = app.config
QueryStatus = utils.QueryStatus


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
    route_base = '/p'

    def get_viz(self, slice_id=None, args=None, datasource_type=None, datasource_id=None,
                database_id=None, full_tb_name=None):
        if slice_id:
            slc = db.session.query(Slice).filter_by(id=slice_id).one()
            return slc.get_viz()
        else:
            viz_type = args.get('viz_type', 'table')
            if database_id and full_tb_name:
                datasource = Dataset.temp_dataset(database_id, full_tb_name)
            else:
                datasource = SourceRegistry.get_datasource(
                    datasource_type, datasource_id, db.session)
            if not datasource:
                raise PropertyException('Missing a dataset for slice')
            if not datasource.database:
                raise PropertyException(
                    'Missing connection for dataset: [{}]'.format(datasource))
            viz_obj = viz.viz_types[viz_type](
                datasource, request.args if request.args else args)
            return viz_obj

    @catch_exception
    @expose("/release/<model>/<action>/<id>/", methods=['GET'])
    def release_object(self, model, action, id):
        """model: dashboard, slice, dataset, database, hdfsconnection
           action: online, offline
           """
        cls = str_to_model.get(model)
        obj = db.session.query(cls).filter_by(id=id).first()
        if not obj:
            msg = _("Not found the object: model={model}, id={id}")\
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

    @catch_exception
    @expose("/slice/<slice_id>/")
    def slice(self, slice_id):
        viz_obj = self.get_viz(slice_id)
        return redirect(viz_obj.get_url(**request.args))

    @catch_exception
    @expose("/explore_json/<datasource_type>/<datasource_id>/")
    def explore_json(self, datasource_type, datasource_id):
        """render the chart of slice"""
        database_id = request.args.get('database_id')
        full_tb_name = request.args.get('full_tb_name')
        slice_id = request.args.get('slice_id')
        try:
            viz_obj = self.get_viz(
                datasource_type=datasource_type,
                datasource_id=datasource_id,
                database_id=database_id,
                full_tb_name=full_tb_name,
                args=request.args)
            self.check_slice_explore_perm(
                slice_id, datasource_id, database_id, full_tb_name)
        except Exception as e:
            logging.exception(e)
            return Response(utils.error_msg_from_exception(e), status=500)

        status = 200
        try:
            payload = viz_obj.get_payload()
        except Exception as e:
            logging.exception(e)
            return Response(utils.error_msg_from_exception(e), status=500)

        if payload.get('status') == QueryStatus.FAILED:
            status = 500
        return Response(
            viz_obj.json_dumps(payload),
            status=status,
            mimetype="application/json")

    @catch_exception
    @expose("/explore/<datasource_type>/<datasource_id>/")
    def explore(self, datasource_type, datasource_id):
        """render the parameters of slice"""
        viz_type = request.args.get("viz_type")
        slice_id = request.args.get('slice_id')
        database_id = request.args.get('database_id')
        full_tb_name = request.args.get('full_tb_name')
        user_id = g.user.id

        slc = None
        if slice_id:
            slc = db.session.query(Slice).filter_by(id=slice_id).first()

        datasets = db.session.query(Dataset).all()
        datasets = sorted(datasets, key=lambda ds: ds.full_name)
        if self.guardian_auth:
            from superset.guardian import guardian_client as client
            if not client.check_global_read(g.user.username):
                readable_names = client.search_model_perms(
                    g.user.username, Dataset.guardian_type)
                readable_datasets = [d for d in datasets if d.name in readable_names]
                datasets = readable_datasets

        try:
            viz_obj = self.get_viz(
                datasource_type=datasource_type,
                datasource_id=datasource_id,
                database_id=database_id,
                full_tb_name=full_tb_name,
                args=request.args)
        except Exception as e:
            raise e

        # slc perms
        slice_add_perm = True
        slice_download_perm = True
        if not slc:
            slice_edit_perm = True
        else:
            slice_edit_perm = self.check_edit_perm(slc.guardian_datasource(),
                                                   raise_if_false=False)
        # handle save or overwrite
        action = request.args.get('action')
        if action in ('saveas', 'overwrite'):
            return self.save_or_overwrite_slice(
                request.args, slc, slice_add_perm, slice_edit_perm)

        is_in_explore_v2_beta = False
        # handle different endpoints
        if request.args.get("csv") == "true":
            payload = viz_obj.get_csv()
            return Response(
                payload,
                status=200,
                headers=self.generate_download_headers("csv"),
                mimetype="application/csv")
        elif request.args.get("standalone") == "true":
            return self.render_template("superset/standalone.html", viz=viz_obj, standalone_mode=True)
        elif request.args.get("V2") == "true" or is_in_explore_v2_beta:
            # bootstrap data for explore V2
            bootstrap_data = {
                "can_add": slice_add_perm,
                "can_download": slice_download_perm,
                "can_edit": slice_edit_perm,
                # TODO: separate endpoint for fetching datasources
                "datasources": [(d.id, d.full_name) for d in datasets],
                "datasource_id": datasource_id,
                "datasource_name": viz_obj.datasource.name,
                "datasource_type": datasource_type,
                "user_id": user_id,
                "viz": json.loads(viz_obj.json_data),
                "filter_select": viz_obj.datasource.filter_select_enabled
            }
            table_name = viz_obj.datasource.table_name \
                if datasource_type == 'table' \
                else viz_obj.datasource.datasource_name
            return self.render_template(
                "superset/explorev2.html",
                bootstrap_data=json.dumps(bootstrap_data),
                slice=slc,
                table_name=table_name)
        else:
            self.update_redirect()
            return self.render_template(
                "superset/explore.html",
                viz=viz_obj,
                slice=slc,
                datasources=datasets,
                can_add=slice_add_perm,
                can_edit=slice_edit_perm,
                can_download=slice_download_perm,
                userid=user_id
            )

    @catch_exception
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
        error_redirect = '/slice/list/'
        datasource_class = Dataset

        datasource = db.session.query(
            datasource_class).filter_by(id=datasource_id).first()

        if not datasource:
            flash(DATASOURCE_MISSING_ERR, "alert")
            raise ParameterException(DATASOURCE_MISSING_ERR)

        viz_type = request.args.get("viz_type")
        if not viz_type and datasource.default_endpoint:
            return redirect(datasource.default_endpoint)
        if not viz_type:
            viz_type = "table"
        try:
            obj = viz.viz_types[viz_type](
                datasource,
                form_data=request.args,
                slice_=None)
        except Exception as e:
            flash(str(e), "danger")
            return redirect(error_redirect)
        status = 200
        payload = obj.get_values_for_column(column)
        return json_response(data=payload)

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

    def save_or_overwrite_slice(self, args, slc, slice_add_perm, slice_edit_perm):
        """Save or overwrite a slice"""
        slice_name = args.get('slice_name')
        action = args.get('action')

        # TODO use form processing form wtforms
        d = args.to_dict(flat=False)
        del d['action']
        if 'previous_viz_type' in d:
            del d['previous_viz_type']

        as_list = ('metrics', 'groupby', 'columns', 'all_columns',
                   'mapbox_label', 'order_by_cols')
        for k in d:
            v = d.get(k)
            if k in as_list and not isinstance(v, list):
                d[k] = [v] if v else []
            if k not in as_list and isinstance(v, list):
                d[k] = v[0]

        datasource_type = args.get('datasource_type')
        datasource_id = args.get('datasource_id')
        database_id = args.get('database_id')
        full_tb_name = args.get('full_tb_name')
        if database_id and full_tb_name:
            datasource_id = None

        if action in ('saveas'):
            d.pop('slice_id')  # don't save old slice_id
            slc = Slice()

        slc.params = json.dumps(d, indent=4, sort_keys=True)
        slc.datasource_name = args.get('datasource_name')
        slc.viz_type = args.get('viz_type')
        slc.datasource_type = datasource_type
        slc.datasource_id = datasource_id if datasource_id else None
        slc.slice_name = slice_name or slc.slice_name
        slc.database_id = database_id if database_id else None
        slc.full_table_name = full_tb_name
        SliceModelView().check_column_values(slc)

        if action in ('saveas') and slice_add_perm:
            self.save_slice(slc)
        elif action == 'overwrite' and slice_edit_perm:
            self.overwrite_slice(slc)
            slc = db.session.query(Slice).filter_by(id=slc.id).first()
            for dash in slc.dashboards:
                dash.need_capture = True

        # Adding slice to a dashboard if requested
        dash = None
        if request.args.get('add_to_dash') == 'existing':
            dash = (
                db.session.query(Dashboard)
                    .filter_by(id=int(request.args.get('save_to_dashboard_id')))
                    .one()
            )
            if dash and slc not in dash.slices:
                dash.slices.append(slc)
                dash.need_capture = True
                db.session.commit()
                Log.log_update(dash, 'dashboard', g.user.id)
            flash(
                _("Slice [{slice}] was added to dashboard [{dashboard}]").format(
                    slice=slc.slice_name,
                    dashboard=dash.name),
                "info")
        elif request.args.get('add_to_dash') == 'new':
            dash = Dashboard(name=request.args.get('new_dashboard_name'))
            if dash and slc not in dash.slices:
                dash.slices.append(slc)
                dash_view = DashboardModelView()
                dash_view._add(dash)
            flash(_("Dashboard [{dashboard}] just got created and slice [{slice}] "
                    "was added to it").format(dashboard=dash.name,
                                              slice=slc.slice_name),
                  "info")

        if request.args.get('goto_dash') == 'true':
            if request.args.get('V2') == 'true':
                return dash.url
            return redirect(dash.url)
        else:
            if request.args.get('V2') == 'true':
                return slc.slice_url
            return redirect(slc.slice_url)

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

    @catch_exception
    @expose("/checkbox/<model_view>/<id_>/<attr>/<value>/", methods=['GET'])
    def checkbox(self, model_view, id_, attr, value):
        """endpoint for checking/unchecking any boolean in a sqla model"""
        views = sys.modules[__name__]
        model_view_cls = getattr(views, model_view)
        model = model_view_cls.datamodel.obj

        obj = db.session.query(model).filter_by(id=id_).first()
        if obj:
            setattr(obj, attr, value == 'true')
            db.session.commit()
        return json_response(data="OK")

    @catch_exception
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

    @catch_exception
    @expose("/tables/<db_id>/<schema>/")
    def tables(self, db_id, schema):
        """endpoint to power the calendar heatmap on the welcome page"""
        schema = None if schema in ('null', 'undefined') else schema
        database = db.session.query(Database).filter_by(id=db_id).one()
        tables = [t for t in database.all_table_names(schema)]
        views = [v for v in database.all_table_names(schema)]
        payload = {'tables': tables, 'views': views}
        return json_response(data=payload)

    @catch_exception
    @expose("/copy_dash/<dashboard_id>/", methods=['GET', 'POST'])
    def copy_dash(self, dashboard_id):
        """Copy dashboard"""
        session = db.session()
        data = json.loads(request.form.get('data'))
        dash = Dashboard()
        original_dash = session.query(Dashboard).filter_by(id=dashboard_id).first()
        dash.name = data['name']
        dash.slices = original_dash.slices
        dash.params = original_dash.params

        self._set_dash_metadata(dash, data)
        session.add(dash)
        session.commit()
        dash_json = dash.json_data
        self.grant_owner_perms(dash.guardian_datasource())
        Number.log_number(g.user.username, Dashboard.model_type)
        Log.log_add(dash, Dashboard.model_type, g.user.id)
        return json_response(data=dash_json)

    @catch_exception
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
        return json_response(message="SUCCESS")

    @staticmethod
    def _set_dash_metadata(dashboard, data):
        positions = data['positions']
        slice_ids = [int(d['slice_id']) for d in positions]
        dashboard.slices = [o for o in dashboard.slices if o.id in slice_ids]
        positions = sorted(data['positions'], key=lambda x: int(x['slice_id']))
        dashboard.position_json = json.dumps(positions, indent=4, sort_keys=True)
        md = dashboard.params_dict
        dashboard.css = data['css']

        if 'filter_immune_slices' not in md:
            md['filter_immune_slices'] = []
        if 'filter_immune_slice_fields' not in md:
            md['filter_immune_slice_fields'] = {}
        md['expanded_slices'] = data['expanded_slices']
        dashboard.json_metadata = json.dumps(md, indent=4)

    @catch_exception
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
        return json_response(message="SLICES ADDED")

    @catch_exception
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

    @catch_exception
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
            .filter_by(id=obj_id)
            .one()
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
        return json_response(data={'count': count})

    @catch_exception
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

    @catch_exception
    @expose("/dashboard/<dashboard_id>/")
    def dashboard(self, dashboard_id):
        """Server side rendering for a dashboard"""
        self.update_redirect()
        session = db.session()
        dash = session.query(Dashboard).filter_by(id=int(dashboard_id)).one()
        dash_edit_perm = self.check_edit_perm(dash.guardian_datasource(),
                                              raise_if_false=False)
        dash_save_perm = dash_edit_perm
        standalone = request.args.get("standalone") == "true"
        context = dict(
            user_id=g.user.get_id(),
            dash_save_perm=dash_save_perm,
            dash_edit_perm=dash_edit_perm,
            standalone_mode=standalone,
            need_capture=dash.need_capture,
        )
        return self.render_template(
            "superset/dashboard.html",
            dashboard=dash,
            context=json.dumps(context),
            standalone_mode=standalone,
        )

    @catch_exception
    @expose("/sqllab_viz/", methods=['POST'])
    def sqllab_viz(self):
        data = json.loads(request.form.get('data'))
        table_name = data.get('datasourceName')
        viz_type = data.get('chartType')
        table = (
            db.session.query(Dataset)
            .filter_by(dataset_name=table_name)
            .first()
        )
        if not table:
            table = Dataset(dataset_name=table_name)
        table.schema = data.get('schema')
        table.database_id = data.get('dbId')
        q = SupersetQuery(data.get('sql'))
        table.sql = q.stripped()
        db.session.add(table)
        db.session.commit()
        self.grant_owner_perms(table.guardian_datasource())
        Number.log_number(g.user.username, Dataset.model_type)
        Log.log_add(table, Dataset.model_type, g.user.id)

        cols = []
        dims = []
        metrics = []
        for column_name, config in data.get('columns').items():
            is_dim = config.get('is_dim', False)
            col = TableColumn(
                column_name=column_name,
                expression=column_name,
                filterable=is_dim,
                groupby=is_dim,
                is_dttm=config.get('is_date', False),
            )
            cols.append(col)
            if is_dim:
                dims.append(col)
            agg = config.get('agg')
            if agg:
                if agg == 'count_distinct':
                    metrics.append(SqlMetric(
                        metric_name="{agg}__{column_name}".format(**locals()),
                        expression="COUNT(DISTINCT {column_name})"
                            .format(**locals()),
                            ))
                else:
                    metrics.append(SqlMetric(
                        metric_name="{agg}__{column_name}".format(**locals()),
                        expression="{agg}({column_name})".format(**locals()),
                    ))
        if not metrics:
            metrics.append(SqlMetric(
                metric_name="count".format(**locals()),
                expression="count(*)".format(**locals()),
            ))
        table.ref_columns = cols
        table.ref_metrics = metrics
        db.session.commit()
        params = {
            'viz_type': viz_type,
            'groupby': dims[0].column_name if dims else None,
            'metrics': metrics[0].metric_name if metrics else None,
            'metric': metrics[0].metric_name if metrics else None,
            'since': '100 years ago',
            'limit': '0',
            'datasource_id': '{}'.format(table.id),
        }
        params = "&".join([k + '=' + v for k, v in params.items() if v])
        return '/p/explore/table/{table.id}/?{params}'.format(**locals())

    @catch_exception
    @expose("/table/<database_id>/<table_name>/<schema>/")
    def table(self, database_id, table_name, schema):
        schema = None if schema in ('null', 'undefined') else schema
        mydb = db.session.query(Database).filter_by(id=database_id).one()
        cols = []
        try:
            t = mydb.get_columns(table_name, schema)
            if mydb.backend.lower() == 'inceptor':
                indexes, primary_key, foreign_keys = [], [], []
            else:
                indexes = mydb.get_indexes(table_name, schema)
                primary_key = mydb.get_pk_constraint(table_name, schema)
                foreign_keys = mydb.get_foreign_keys(table_name, schema)
        except Exception as e:
            raise DatabaseException(str(e))
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

        for col in t:
            dtype = ""
            try:
                dtype = '{}'.format(col['type'])
            except:
                pass
            cols.append({
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
            'columns': cols,
            'selectStar': mydb.select_sql(
                table_name, schema=schema, show_cols=True, indent=True),
            'primaryKey': primary_key,
            'foreignKeys': foreign_keys,
            'indexes': keys,
        }
        return json_response(data=tbl)

    @catch_exception
    @expose("/extra_table_metadata/<database_id>/<table_name>/<schema>/")
    def extra_table_metadata(self, database_id, table_name, schema):
        # schema = None if schema in ('null', 'undefined') else schema
        # mydb = db.session.query(Database).filter_by(id=database_id).one()
        # payload = mydb.db_engine_spec.extra_table_metadata(
        #     mydb, table_name, schema)
        return json_response(data={})

    @expose("/theme/")
    def theme(self):
        return self.render_template('superset/theme.html')

    @catch_exception
    @expose("/results/<key>/")
    def results(self, key):
        """Serves a key off of the results backend"""
        if not results_backend:
            return json_response(message="Results backend isn't configured",
                                 status=500)
        blob = results_backend.get(key)
        if blob:
            json_payload = zlib.decompress(blob)
            return json_response(data=json_payload)
        else:
            return json_response(
                message="Data could not be retrived. You may want to re-run the query.",
                status=410,
                code=1
            )

    @catch_exception
    @expose("/sql_json/", methods=['POST', 'GET'])
    def sql_json(self):
        """Runs arbitrary sql and returns and json"""
        sql = request.form.get('sql')
        database_id = request.form.get('database_id')
        schema = request.form.get('schema', None)

        session = db.session()
        mydb = session.query(Database).filter_by(id=database_id).first()
        if not mydb:
            raise PropertyException(
                'Database with id {} is missing.'.format(database_id))

        tmp_table_name = request.form.get('tmp_table_name')
        # select_as_cta = request.form.get('select_as_cta') == 'true'
        # if select_as_cta and mydb.force_ctas_schema:
        #     tmp_table_name = '{}.{}'.format(
        #         mydb.force_ctas_schema,
        #         tmp_table_name
        #     )

        query = Query(
            database_id=int(database_id),
            limit=int(app.config.get('SQL_MAX_ROW', 100)),
            sql=sql,
            schema=schema,
            select_as_cta=request.form.get('select_as_cta') == 'true',
            start_time=utils.now_as_float(),
            tab_name=request.form.get('tab'),
            status=QueryStatus.RUNNING,
            sql_editor_id=request.form.get('sql_editor_id'),
            tmp_table_name=tmp_table_name,
            user_id=int(g.user.get_id()),
            client_id=request.form.get('client_id'),
        )
        session.add(query)
        session.commit()

        data = sql_lab.get_sql_results(query.id, return_results=True)
        return json_response(data=json.loads(data))

    @catch_exception
    @expose("/csv/<client_id>/")
    def csv(self, client_id):
        """Download the query results as csv.
        For inceptor, pilot will create a temp table stored as csv file.
        If the size of results is too large, pilot will retain the data files
        in HDFS folder for a period of time, and then drop the temp table.
        """
        with_header = request.args.get('header') == 'true'
        session = db.session()
        query = session.query(Query).filter_by(client_id=client_id).one()
        sql = query.select_sql or query.sql
        database = query.database
        session.close()

        stored_in_hdfs = True if database.is_inceptor else False
        filename = quote('{}.csv'.format(query.name), encoding="utf-8")

        if stored_in_hdfs:
            # Beacuse of JVM attached threads, can't get super user 'pilot' 's token
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
            df = query.database.get_df(sql, query.schema)
            # TODO(bkyryliuk): add compression=gzip for big files.
            csv = df.to_csv(index=False, header=with_header, encoding='utf-8')
            response = Response(csv, mimetype='text/csv')
            response.headers['Content-Disposition'] = \
                'attachment; filename={}'.format(filename)
            return response

    @catch_exception
    @expose("/queries/<last_updated_ms>/")
    def queries(self, last_updated_ms):
        """Get the updated queries."""
        # Unix time, milliseconds.
        last_updated_ms_int = int(float(last_updated_ms)) if last_updated_ms else 0

        # UTC date time, same that is stored in the DB.
        last_updated_dt = utils.EPOCH + timedelta(seconds=last_updated_ms_int / 1000)

        sql_queries = (
            db.session.query(Query)
            .filter(
                Query.user_id == g.user.id,
                Query.changed_on >= last_updated_dt,
                )
            .all()
        )
        dict_queries = {q.client_id: q.to_dict() for q in sql_queries}
        return json_response(data=dict_queries)

    @catch_exception
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

        return json_response(data=dict_queries)

    @catch_exception
    @expose("/sqllab/")
    def sqllab(self):
        """SQL Editor"""
        d = {
            'defaultDbId': None,
        }
        self.update_redirect()
        return self.render_template(
            'superset/sqllab.html',
            bootstrap_data=json.dumps(d, default=utils.json_iso_dttm_ser)
        )
