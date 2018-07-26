import json
import pickle
import time
from datetime import datetime
from flask import g, request, Response
from flask_babel import lazy_gettext as _
from flask_appbuilder import expose
from flask_appbuilder.models.sqla.interface import SQLAInterface
from flask_appbuilder.security.sqla.models import User
from sqlalchemy import and_, or_

from superset import app, db, utils
from superset.exception import ParameterException, GuardianException
from superset.models import (
    Database, Dataset, Slice, Dashboard, Log, FavStar, str_to_model, model_name_columns
)
from superset.message import *
from .base import (
    SupersetModelView, PermissionManagement, catch_exception, json_response
)


config = app.config
QueryStatus = utils.QueryStatus


class DashboardModelView(SupersetModelView, PermissionManagement):
    model = Dashboard
    model_type = model.model_type
    datamodel = SQLAInterface(Dashboard)
    route_base = '/dashboard'
    list_columns = ['id', 'name', 'url', 'description', 'changed_on']
    edit_columns = ['name', 'description']
    show_columns = ['id', 'name', 'description']
    add_columns = edit_columns
    list_template = "superset/partials/dashboard/dashboard.html"

    str_to_column = {
        'title': Dashboard.name,
        'description': Dashboard.description,
        'changed_on': Dashboard.changed_on,
        'owner': User.username,
        'created_by_user': User.username
    }
    int_columns = ['id', 'created_by_fk', 'changed_by_fk']
    bool_columns = ['online']
    str_columns = ['created_on', 'changed_on']

    def pre_add(self, obj):
        self.check_column_values(obj)
        utils.validate_json(obj.json_metadata)
        utils.validate_json(obj.position_json)

    def pre_update(self, old_obj, new_obj):
        self.check_edit_perm(old_obj.guardian_datasource())
        self.pre_add(new_obj)

    def post_delete(self, obj):
        super(DashboardModelView, self).post_delete(obj)
        db.session.query(FavStar) \
            .filter(FavStar.class_name.ilike(self.model_type),
                    FavStar.obj_id == obj.id) \
            .delete(synchronize_session=False)
        db.session.commit()

    def check_column_values(self, obj):
        if not obj.name:
            raise ParameterException(NONE_DASHBOARD_NAME)
        self.model.check_name(obj.name)

    def get_object_list_data(self, **kwargs):
        """
        Return the dashbaords with column 'favorite' and 'online'
        """
        order_column = kwargs.get('order_column')
        order_direction = kwargs.get('order_direction')
        page = kwargs.get('page')
        page_size = kwargs.get('page_size')
        only_favorite = kwargs.get('only_favorite')

        query = self.query_with_favorite(self.model_type, **kwargs)
        query = query.filter(Dashboard.type == Dashboard.data_types[0])

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
        for obj, username, fav_id in rs:
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
                if col == 'changed_on':
                    t = getattr(obj, col, datetime(1970, 1, 1))
                    line['changed_time'] = time.mktime(t.timetuple())
                if col in self.str_columns:
                    line[col] = str(getattr(obj, col, None))
                else:
                    line[col] = getattr(obj, col, None)
            # image_bytes = getattr(obj, 'image', None)
            # image_bytes = b'' if image_bytes is None else image_bytes
            # line['image'] = str(image_bytes, encoding='utf8')
            line['created_by_user'] = username
            line['favorite'] = True if fav_id else False
            data.append(line)

        response = {}
        response['count'] = count
        response['order_column'] = order_column
        response['order_direction'] = 'desc' if order_direction == 'desc' else 'asc'
        response['page'] = page
        response['page_size'] = page_size
        response['only_favorite'] = only_favorite
        response['data'] = data
        return response

    def get_show_attributes(self, obj, user_id=None):
        attributes = super(DashboardModelView, self).get_show_attributes(obj)
        attributes['slices'] = self.slices_to_dict(obj.slices)
        return attributes

    def get_add_attributes(self, data, user_id):
        attributes = super(DashboardModelView, self).get_add_attributes(data, user_id)
        attributes['slices'] = self.get_slices_in_list(data.get('slices'))
        return attributes

    def get_edit_attributes(self, data, user_id):
        attributes = super(DashboardModelView, self).get_edit_attributes(data, user_id)
        attributes['slices'] = self.get_slices_in_list(data.get('slices'))
        attributes['need_capture'] = True
        return attributes

    def get_slices_in_list(self, slices_list):
        ids = [slice_dict.get('id') for slice_dict in slices_list]
        objs = db.session.query(Slice) \
            .filter(Slice.id.in_(ids)).all()
        if len(ids) != len(objs):
            msg = _("Error parameter ids: {ids}, queried {num} slice(s)") \
                .format(ids=ids, num=len(objs))
            self.handle_exception(404, Exception, msg)
        return objs

    @catch_exception
    @expose("/online_info/<id>/", methods=['GET'])
    def online_info(self, id):  # Deprecated
        """
        Changing dashboard to online will make myself slices,_datasets and
        connections online.
        """
        dashboard = self.get_object(id)
        self.check_release_perm(dashboard.guardian_datasource())

        slices = dashboard.slices
        datasets = []
        database_ids = []
        for s in slices:
            if s.datasource_id and s.datasource:
                datasets.append(s.datasource)
            elif s.database_id:
                database_ids.append(s.database_id)

        connections = []
        for d in datasets:
            if d.database:
                connections.append(d.database)
            if d.hdfs_table and d.hdfs_table.hdfs_connection:
                connections.append(d.hdfs_table.hdfs_connection)
        databases = db.session.query(Database) \
            .filter(Database.id.in_(database_ids)) \
            .all()
        connections.extend(databases)

        offline_slices = [s for s in slices
                          if s.online is False and s.created_by_fk == g.user.id]
        offline_datasets = [d for d in datasets
                            if d.online is False and d.created_by_fk == g.user.id]
        offline_connections = [c for c in connections
                               if c.online is False and c.created_by_fk == g.user.id]

        info = _("Releasing dashboard {dashboard} will release these too: "
                 "\nSlice: {slice}, \nDataset: {dataset}, \nConnection: {connection}") \
            .format(dashboard=[dashboard, ],
                    slice=offline_slices,
                    dataset=offline_datasets,
                    connection=offline_connections)
        return json_response(data=info)

    @catch_exception
    @expose("/offline_info/<id>/", methods=['GET'])
    def offline_info(self, id):  # Deprecated
        dash = self.get_object(id)
        self.check_release_perm(dash.guardian_datasource())
        info = _("Changing dashboard {dashboard} to offline will make it invisible "
                 "for other users").format(dashboard=[dash, ])
        return json_response(data=info)

    @catch_exception
    @expose("/delete_info/<id>/", methods=['GET'])
    def delete_info(self, id):
        dash = self.get_object(id)
        self.check_delete_perm(dash.guardian_datasource())
        return json_response(data='')

    @catch_exception
    @expose("/muldelete_info/", methods=['POST'])
    def muldelete_info(self):
        return json_response(data='')

    @catch_exception
    @expose("/grant_info/<id>/", methods=['GET'])
    def grant_info(self, id):
        dash = self.get_object(id)
        self.check_grant_perm(dash.guardian_datasource())
        objects = self.grant_affect_objects(dash)
        info = _("Granting permissions of [{dashboard}] to this user, will grant "
                 "permissions of dependencies to this user too: "
                 "\nSlice: {slice}, \nDataset: {dataset}, \nConnection: {connection}") \
            .format(dashboard=dash,
                    slice=objects.get('slice'),
                    dataset=objects.get('dataset'),
                    connection=objects.get('connection'))
        return json_response(data=info)

    def grant_affect_objects(self, dash):
        slices = dash.slices
        datasets = []
        database_ids = []
        for s in slices:
            if s.datasource_id and s.datasource:
                datasets.append(s.datasource)
            elif s.database_id:
                database_ids.append(s.database_id)

        connections = []
        for d in datasets:
            if d.database and d.database.name != self.main_db_name:
                connections.append(d.database)
            if d.hdfs_table and d.hdfs_table.hdfs_connection:
                connections.append(d.hdfs_table.hdfs_connection)
        databases = db.session.query(Database) \
            .filter(Database.id.in_(database_ids),
                    Database.database_name != self.main_db_name) \
            .all()
        connections.extend(databases)
        return {'slice': list(set(slices)),
                'dataset': list(set(datasets)),
                'connection': list(set(connections))}

    @catch_exception
    @expose("/before_import/", methods=['POST'])
    def before_import(self):
        """Before import, check same names of objects in file and database.
        #:return {'dashboard': {'order': 1,
                                'abbr': 'Dashboard',  # translation
                                'names': {'n1': {'can_overwrite': true},
                                          'n2': {'can_overwrite': false},
                                        }
                                }
                'slice' : {...}
                 }
        """
        f = request.data
        data = pickle.loads(f)
        import_objs, same_objs = {}, {}
        for obj_type in self.OBJECT_TYPES:
            import_objs[obj_type] = []
        same_objs = {'dashboard': {'order': 1, 'abbr': str(_('Dashboard')), 'names': {}},
                     'slice': {'order': 2, 'abbr': str(_('Slice')), 'names': {}},
                     'dataset': {'order': 3, 'abbr': str(_('Dataset')), 'names': {}},
                     'database': {'order': 4, 'abbr': str(_('DB connection')), 'names': {}},
                     'hdfsconnection': {'order': 5, 'abbr': str(_('HDFS connection')), 'names': {}},
                     }

        for dataset in data['datasets']:
            import_objs[self.OBJECT_TYPES[2]].append(dataset.name)
            if dataset.database:
                import_objs[self.OBJECT_TYPES[0]].append(dataset.database.name)
            if dataset.hdfs_table and dataset.hdfs_table.hdfs_connection:
                import_objs[self.OBJECT_TYPES[1]].append(
                    dataset.hdfs_table.hdfs_connection.name)
        for dashboard in data['dashboards']:
            import_objs[self.OBJECT_TYPES[4]].append(dashboard.name)
            for slice in dashboard.slices:
                import_objs[self.OBJECT_TYPES[3]].append(slice.name)
                if slice.database_id and slice.database:
                    import_objs[self.OBJECT_TYPES[0]].append(slice.database.name)

        for obj_type, obj_names in import_objs.items():
            if obj_names:
                model = str_to_model[obj_type]
                name_column = model_name_columns[obj_type]
                sames = db.session.query(model).filter(name_column.in_(obj_names)).all()
                for o in sames:
                    editable = self.check_edit_perm(o.guardian_datasource(),
                                                    raise_if_false=False)
                    same_objs[obj_type]['names'][o.name] = {'can_overwrite': editable}

        same_objs = dict((k, v) for k, v in same_objs.items() if v['names'])
        return json_response(data=same_objs)

    @catch_exception
    @expose("/import/", methods=['GET', 'POST'])
    def import_dashboards(self):
        """Import dashboards and dependencies"""
        solution = json.loads(request.args.get('param', '{}'))
        session = db.session
        Dashboard.check_solution(solution, db.session, str_to_model)

        f = request.data
        objects = pickle.loads(f)
        folders_dict = objects.get('folders', None)

        folder_ids_dict = {}
        if folders_dict:
            folder_ids_dict = Dashboard.import_folders(folders_dict)

        for dataset in objects['datasets']:
            Dataset.import_obj(
                session, dataset, solution, self.grant_owner_perms)
        for dashboard in objects['dashboards']:
            Dashboard.import_obj(session, dashboard, solution,
                                 self.grant_owner_perms, folder_ids_dict)
            Log.log('import', dashboard, 'dashboard', g.user.id)
        return json_response('Import success')

    @catch_exception
    @expose("/export/")
    def export_dashboards(self):
        ids = request.args.get('ids')
        ids = eval(ids)
        if isinstance(ids, int):
            ids = [ids, ]
        return Response(
            Dashboard.export_dashboards(ids),
            headers=self.generate_download_headers("pickle"),
            mimetype="application/text")

    def add_slices_api(self, dashboard_id, slice_ids):
        """Add and save slices to a dashboard"""
        session = db.session()
        dash = session.query(Dashboard).filter_by(id=dashboard_id).first()
        self.check_edit_perm(dash.guardian_datasource())
        new_slices = session.query(Slice).filter(
            Slice.id.in_(slice_ids))
        dash.slices += new_slices
        session.merge(dash)
        session.commit()
        session.close()
        return True

    @catch_exception
    @expose("/folder/listdata/", methods=['GET'])
    def list_folder_data(self):
        folder_id = request.args.get('folder_id')
        present_folder = None
        if folder_id:
            present_folder = Dashboard.get_folder(folder_id)
        data = {'folder_id': folder_id,
                'folder_name': present_folder.name if present_folder else None,
                'folders': [],
                'dashboards': []}

        if not folder_id:
            query = db.session.query(Dashboard) \
                .filter(
                    or_(
                        and_(
                            Dashboard.type == Dashboard.data_types[0],
                            Dashboard.path == None
                        ),
                        and_(
                            Dashboard.type == Dashboard.data_types[1],
                            ~Dashboard.path.like('%/%')
                        )
                    )
                )
        else:
            query = db.session.query(Dashboard) \
                .filter(
                    or_(
                        and_(
                            Dashboard.type == Dashboard.data_types[0],
                            Dashboard.path == folder_id
                        ),
                        and_(
                            Dashboard.type == Dashboard.data_types[1],
                            Dashboard.path.like('%{}/%'.format(folder_id)),
                            Dashboard.path.like('%/{}/%'.format(folder_id)),
                            ~Dashboard.path.like('{}/%/%'.format(folder_id)),
                            ~Dashboard.path.like('%/{}/%/%'.format(folder_id)),
                        )
                    )
                )
        query = query.order_by(Dashboard.name.desc())

        for d in query.all():
            if d.type == Dashboard.data_types[0]:  # dashboard
                if self.check_read_perm(d.guardian_datasource(), raise_if_false=False):
                    data['dashboards'].append({'id': d.id, 'name': d.name})
            elif d.type == Dashboard.data_types[1]:  # folder
                data['folders'].append({'id': d.id, 'name': d.name})

        return json_response(data=data)

    @catch_exception
    @expose("/folder/add/", methods=['GET'])
    def add_folder(self):
        parent_id = request.args.get('parent_id')
        name = request.args.get('name')
        Dashboard.add_folder(name, parent_id)
        return json_response(message=ADD_SUCCESS)

    @catch_exception
    @expose("/folder/rename/", methods=['GET'])
    def rename_folder(self):
        folder_id = request.args.get('folder_id')
        new_name = request.args.get('new_name')
        folder = Dashboard.get_folder(folder_id)
        if folder.name != new_name:
            self.check_folder_perm(folder, 'rename')
            folder.name = new_name
            db.session.commit()
        # TODO rename guardian datasource
        return json_response(message=UPDATE_SUCCESS)

    @catch_exception
    @expose("/folder/delete/", methods=['GET'])
    def delete_folder(self):
        folder_id = request.args.get('folder_id')
        folder = Dashboard.get_folder(folder_id)
        folders, dashs = self.check_folder_perm(folder, 'delete')

        for dash in dashs:
            self._delete(dash)
        for f in folders:
            db.session.delete(f)
        db.session.commit()
        return json_response(message=DELETE_SUCCESS)

    @catch_exception
    @expose("/folder/move/dashboard/", methods=['GET'])
    def move_dashboard(self):
        dash_id = request.args.get('dashboard_id')
        folder_id = request.args.get('folder_id')

        dash = Dashboard.get_object(id=dash_id)
        self.check_edit_perm(dash.guardian_datasource())
        if folder_id:
            Dashboard.get_folder(folder_id)  # ensure folder is existed
            dash.path = folder_id
        else:  # is None, move to root path
            dash.path = None
        db.session.commit()
        # TODO modify guardian datasource
        return json_response(message=MOVE_DASHBOARD_SUCCESS)

    @catch_exception
    @expose("/folder/move/folder/", methods=['GET'])
    def move_folder(self):
        folder_id = request.args.get('folder_id')
        parent_folder_id = request.args.get('parent_folder_id')

        folder = Dashboard.get_folder(folder_id)
        if parent_folder_id:
            parent_folder = Dashboard.get_folder(parent_folder_id)
            if parent_folder.path.startswith(folder.path):
                raise ParameterException(MOVE_FOLDER_TO_CHILD_ERROR)

            related_folders, _ = self.check_folder_perm(folder, 'move')
            old_parent_path = folder.get_parent_path()
            if old_parent_path != parent_folder.path:
                for rf in related_folders:
                    rf.path = rf.path.replace(old_parent_path, parent_folder.path)
                db.session.commit()
        else:  # move to root
            folder.path = '{}'.format(folder_id)
            db.session.commit()
        # TODO modify guardian datasource
        return json_response(message=MOVE_FOLDER_SUCCESS)

    def check_folder_perm(self, folder, action):
        """
        :param folder: the folder object
        :param action: could be 'rename', 'move', 'delete'
        :return: folders including present and children folders, and dashboards in
        these folders
        """
        folders = db.session.query(Dashboard)\
            .filter(
                Dashboard.type == Dashboard.data_types[1],
                or_(Dashboard.path.like('{}'.format(folder.id)),
                    Dashboard.path.like('{}/%'.format(folder.id)),
                    Dashboard.path.like('%/{}'.format(folder.id)),
                    Dashboard.path.like('%/{}/%'.format(folder.id)))
            )\
            .all()
        folder_ids = ['{}'.format(f.id) for f in folders]

        dashs = db.session.query(Dashboard).filter(Dashboard.path.in_(folder_ids)).all()
        if action in ['rename', 'move']:
            for dash in dashs:
                if not self.check_edit_perm(dash.guardian_datasource(),
                                            raise_if_false=False):
                    raise GuardianException(
                        _('No privilege to edit [{dash}], so cannot edit folder [{folder}]')
                            .format(dash=dash, folder=folder))
        elif action in ['delete', ]:
            for dash in dashs:
                if not self.check_delete_perm(dash.guardian_datasource(),
                                              raise_if_false=False):
                    raise GuardianException(
                        _('No privilege to delete [{dash}], so cannot delete folder [{folder}]')
                            .format(dash=dash, folder=folder))
        return folders, dashs

    @catch_exception
    @expose("/folder/tree/", methods=['GET'])
    def directory_tree(self):
        folder_id = request.args.get('folder_id', None)
        tree = Dashboard.tree_dict(folder_id)
        return json_response(data=tree)
