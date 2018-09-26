# -*- coding: utf-8 -*-
# pylint: disable=C,R,W
"""A collection of ORM sqlalchemy models for Pilot"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import json
import logging
import pickle
from copy import copy

from flask import escape, Markup
from flask_babel import lazy_gettext as _
from flask_appbuilder import Model
from sqlalchemy import (
    Column, Integer, String, ForeignKey, Text, Boolean, LargeBinary, Table,
    MetaData, UniqueConstraint
)
from sqlalchemy.orm import relationship, subqueryload
from sqlalchemy.orm.session import make_transient
from urllib import parse

from superset import app, db, security_manager
from superset.message import DUPLICATE_NAME
from superset.exceptions import ParameterException, PropertyException
from .base import AuditMixinNullable, ImportMixin
from .slice import Slice
from .dataset import Dataset


config = app.config

dashboard_slices = Table(
    'dashboard_slices', Model.metadata,
    Column('id', Integer, primary_key=True),
    Column('dashboard_id', Integer, ForeignKey('dashboards.id')),
    Column('slice_id', Integer, ForeignKey('slices.id')),
)

dashboard_user = Table(
    'dashboard_user', Model.metadata,
    Column('id', Integer, primary_key=True),
    Column('user_id', Integer, ForeignKey('ab_user.id')),
    Column('dashboard_id', Integer, ForeignKey('dashboards.id')),
)


class Dashboard(Model, AuditMixinNullable, ImportMixin):
    __tablename__ = 'dashboards'
    model_type = 'dashboard'
    guardian_type = model_type.upper()
    data_types = ['dashboard', 'folder']
    max_depth = 4

    id = Column(Integer, primary_key=True)
    name = Column(String(128), nullable=False)
    position_json = Column(Text)
    description = Column(Text)
    department = Column(String(256))
    css = Column(Text)
    online = Column(Boolean, default=False)
    json_metadata = Column(Text)
    slug = Column(String(128))
    image = Column(LargeBinary(length=(2**32)-1), nullable=True)  # dashboard thumbnail
    need_capture = Column(Boolean, default=True)  # if need new thumbnail
    type = Column(String(12), default='dashboard')  # values in ['dashboard', 'folder']
    path = Column(String(128))
    slices = relationship('Slice', secondary=dashboard_slices, backref='dashboards')
    owners = relationship(security_manager.user_model, secondary=dashboard_user)

    __table_args__ = (
        UniqueConstraint('name', name='dashboard_title_uc'),
    )

    export_fields = ('name', 'position_json', 'description', 'online',
                     'json_metadata', 'image', 'need_capture')

    def __repr__(self):
        return self.name

    @classmethod
    def name_column(cls):
        return cls.name

    @property
    def datasets(self):
        d = []
        for s in self.slices:
            if s.datasource:
                d.append(str(s.datasource))
        return ", ".join(set(d))

    def guardian_datasource(self):
        if self.type != self.data_types[0]:
            logging.exception('No guardian datasource for dashboard (type={})'
                              .format(self.type))
            return None
        elif not self.path:
            return [self.model_type.upper(), self.name]
        else:
            folder = db.session.query(Dashboard) \
                .filter(Dashboard.id == self.path,
                        Dashboard.type == Dashboard.data_types[1]
                        ).first()
            if not folder:
                logging.exception('Not existed dashboard(folder) with id={}'
                                  .format(self.path))
                return [self.model_type.upper(), self.name]
            else:
                return [self.model_type.upper(), ] + folder.real_path() + [self.name, ]

    @property
    def url(self):
        if self.json_metadata:
            # add default_filters to the preselect_filters of dashboard
            json_metadata = json.loads(self.json_metadata)
            default_filters = json_metadata.get('default_filters')
            # make sure default_filters is not empty and is valid
            if default_filters and default_filters != '{}':
                try:
                    if json.loads(default_filters):
                        filters = parse.quote(default_filters.encode('utf8'))
                        return '/superset/dashboard/{}/?preselect_filters={}'.format(
                            self.slug or self.id, filters)
                except Exception:
                    pass
        return '/superset/dashboard/{}/'.format(self.id)

    @property
    def datasources(self):
        return {slc.datasource for slc in self.slices}

    @property
    def sqla_metadata(self):
        # pylint: disable=no-member
        metadata = MetaData(bind=self.get_sqla_engine())
        return metadata.reflect()

    def dashboard_link(self):
        title = escape(self.name)
        return Markup(
            '<a href="{self.url}">{title}</a>'.format(**locals()))

    @property
    def data(self):
        positions = self.position_json
        if positions:
            positions = json.loads(positions)
        return {
            'id': self.id,
            'metadata': self.params_dict,
            'css': self.css,
            'dashboard_title': self.name,
            'slug': self.slug,
            'slices': [slc.data for slc in self.slices],
            'position_json': positions,
        }

    @property
    def params(self):
        return self.json_metadata

    @params.setter
    def params(self, value):
        self.json_metadata = value

    @property
    def position_array(self):
        if self.position_json:
            return json.loads(self.position_json)
        return []

    @classmethod
    def import_obj(cls, session, i_dash, solution, grant_owner_perms,
                   folder_ids_dict):
        """Imports the dashboard from the object to the database.
        """
        def alter_positions(dashboard, old_to_new_slc_id_dict):
            """ Updates slice_ids in the position json.

            Sample position json v1:
            [{
                "col": 5,
                "row": 10,
                "size_x": 4,
                "size_y": 2,
                "slice_id": "3610"
            }]

            Sample position json v2:
            {
                "DASHBOARD_VERSION_KEY": "v2",
                "DASHBOARD_ROOT_ID": {
                    "type": "DASHBOARD_ROOT_TYPE",
                    "id": "DASHBOARD_ROOT_ID",
                    "children": ["DASHBOARD_GRID_ID"]
                },
                "DASHBOARD_GRID_ID": {
                    "type": "DASHBOARD_GRID_TYPE",
                    "id": "DASHBOARD_GRID_ID",
                    "children": ["DASHBOARD_CHART_TYPE-2"]
                },
                "DASHBOARD_CHART_TYPE-2": {
                    "type": "DASHBOARD_CHART_TYPE",
                    "id": "DASHBOARD_CHART_TYPE-2",
                    "children": [],
                    "meta": {
                        "width": 4,
                        "height": 50,
                        "chartId": 118
                    }
                },
            }
            """
            position_data = json.loads(dashboard.position_json)
            is_v2_dash = (
                isinstance(position_data, dict) and
                position_data.get('DASHBOARD_VERSION_KEY') == 'v2'
            )
            if is_v2_dash:
                position_json = position_data.values()
                for value in position_json:
                    if (isinstance(value, dict) and value.get('meta') and
                            value.get('meta').get('chartId')):
                        old_slice_id = value.get('meta').get('chartId')

                        if old_slice_id in old_to_new_slc_id_dict:
                            value['meta']['chartId'] = (
                                old_to_new_slc_id_dict[old_slice_id]
                            )
                dashboard.position_json = json.dumps(position_data)
            else:
                position_array = dashboard.position_array
                for position in position_array:
                    if 'slice_id' not in position:
                        continue
                    old_slice_id = int(position['slice_id'])
                    if old_slice_id in old_to_new_slc_id_dict:
                        position['slice_id'] = '{}'.format(
                            old_to_new_slc_id_dict[old_slice_id])
                dashboard.position_json = json.dumps(position_array)

        slices = copy(i_dash.slices)
        old_to_new_slc_id_dict = {}
        new_filter_immune_slices = []
        new_expanded_slices = {}
        i_params_dict = i_dash.params_dict

        for slc in slices:
            old_slc_id = slc.id
            new_slice = Slice.import_obj(session, slc, solution, grant_owner_perms)
            old_to_new_slc_id_dict[old_slc_id] = new_slice.id
            # update json metadata that deals with slice ids
            new_slc_id = '{}'.format(new_slice.id)
            old_slc_id = '{}'.format(old_slc_id)
            if 'filter_immune_slices' in i_params_dict \
                    and old_slc_id in i_params_dict['filter_immune_slices']:
                new_filter_immune_slices.append(new_slc_id)
            if 'expanded_slices' in i_params_dict \
                    and old_slc_id in i_params_dict['expanded_slices']:
                new_expanded_slices[new_slc_id] = \
                    (i_params_dict['expanded_slices'][old_slc_id])

        alter_positions(i_dash, old_to_new_slc_id_dict)
        if new_expanded_slices:
            i_dash.alter_params(expanded_slices=new_expanded_slices)
        if new_filter_immune_slices:
            i_dash.alter_params(filter_immune_slices=new_filter_immune_slices)

        new_slices = session.query(Slice) \
            .filter(Slice.id.in_(old_to_new_slc_id_dict.values())) \
            .all()
        new_path = folder_ids_dict.get(i_dash.path, None)

        i_dash.id = None
        existed_dash = cls.get_object(name=i_dash.name)
        new_dash = existed_dash

        if not existed_dash:
            logging.info('Importing dashboard: [{}] (add)'.format(i_dash))
            new_dash = i_dash.copy()
            new_dash.slices = new_slices
            new_dash.path = new_path
            session.commit()
            grant_owner_perms([cls.guardian_type, new_dash.name])
        else:
            policy, new_name = cls.get_policy(cls.model_type, i_dash.name, solution)
            if policy == cls.Policy.OVERWRITE:
                logging.info('Importing dashboard: [{}] (overwrite)'.format(i_dash))
                new_dash.override(i_dash)
                new_dash.slices = new_slices
                new_dash.path = new_path
                session.commit()
            elif policy == cls.Policy.RENAME:
                logging.info('Importing dashboard: [{}] (rename to [{}])'
                             .format(i_dash, new_name))
                new_dash = i_dash.copy()
                new_dash.name = new_name
                new_dash.slices = new_slices
                new_dash.path = new_path
                session.commit()
                grant_owner_perms([cls.guardian_type, new_dash.name])
            elif policy == cls.Policy.SKIP:
                logging.info('Importing dashboard: [{}] (skip)'.format(i_dash))

        return new_dash

    @classmethod
    def export_dashboards(cls, dashboard_ids):
        copied_dashs, copied_datasets = [],  []
        dataset_ids, folder_ids = set(), set()

        for dashboard_id in dashboard_ids:
            dashboard_id = int(dashboard_id)
            copied_dashboard = (
                db.session.query(Dashboard)
                    .options(subqueryload(Dashboard.slices))
                    .filter_by(id=dashboard_id).first()
            )
            make_transient(copied_dashboard)
            for slc in copied_dashboard.slices:
                if slc.datasource_id and slc.datasource:
                    dataset = slc.datasource
                    dataset_ids.add(dataset.id)

            copied_dashs.append(copied_dashboard)
            if copied_dashboard.type == Dashboard.data_types[0] and \
                    copied_dashboard.path:
                folder_ids.add(copied_dashboard.path)

        for id in dataset_ids:
            dataset = (
                db.session.query(Dataset)
                    .options(subqueryload(Dataset.ref_columns),
                             subqueryload(Dataset.ref_metrics))
                    .filter_by(id=id).first()
            )
            make_transient(dataset)
            copied_datasets.append(dataset)

        folders = cls.get_ancestors_tree(folder_ids)
        return pickle.dumps({
            'folders': folders,
            'dashboards': copied_dashs,
            'datasets': copied_datasets,
        })

    @classmethod
    def count(cls):
        return db.session.query(cls).filter(cls.type == cls.data_types[0]).count()

    def get_parent_path(self):
        if self.type == self.data_types[0]:
            raise PropertyException('Cannot get parent path for [{}]'.format(self.type))
        match_str = '{}'.format(self.id)
        if not self.path.endswith(match_str):
            raise PropertyException(
                _('Error materialized path [{path}] for folder(id={id})')
                    .format(path=self.path, id=self.id))
        index = self.path.find(match_str)
        if self.path == match_str:
            return None
        else:
            return self.path[:index-1]

    @classmethod
    def get_folder(cls, id):
        folder = db.session.query(Dashboard) \
            .filter(Dashboard.id == id,
                    Dashboard.type == Dashboard.data_types[1]) \
            .first()
        if not folder:
            raise ParameterException('Not existed the folder with id [{}]'.format(id))
        return folder

    @classmethod
    def add_folder(cls, name, parent_id=None):
        """Add child folder with 'name'"""
        cls.check_name(name)
        parent = None
        if parent_id:
            parent = cls.get_folder(parent_id)
            cls.check_path_depth(parent.path)

        folder = Dashboard(name=name, type=Dashboard.data_types[1])
        db.session.add(folder)
        db.session.commit()

        if parent:
            path = '{}/{}'.format(parent.path, folder.id)
        else:
            path = '{}'.format(folder.id)
        folder.path = path
        db.session.commit()
        return folder

    @classmethod
    def check_path_depth(cls, path):
        depth = path.count('/')
        if depth >= cls.max_depth:
            raise ParameterException(
                _("Folders' depth is limited to [{depth}]").format(depth=cls.MAX_DEPTH))

    def real_path(self):
        if self.type != self.data_types[1]:
            raise PropertyException('Dashboard does not have path')
        if str(self.id) == self.path:
            return [self.name, ]
        else:
            ids = self.path.split('/')
            ids = [int(i) for i in ids]
            folders = db.session.query(Dashboard) \
                .filter(Dashboard.id.in_(ids),
                        Dashboard.type == Dashboard.data_types[1]) \
                .all()
            names_dict = {}
            for f in folders:
                names_dict[f.id] = f.path
            return [names_dict.get(i) for i in ids]

    @classmethod
    def tree_dict(cls, folder_id=None):
        """Get the tree of folder with 'folder_id' or root"""
        tree = []
        if folder_id:
            folder = cls.get_folder(folder_id)
            d = {'id': folder.id,
                 'name': folder.name,
                 'children': cls.children_dict(folder)}
            tree.append(d)
        else:
            tree = cls.children_dict()
        return tree

    @classmethod
    def children_dict(cls, folder=None):
        """Recursive method to get children tree as dict"""
        children_list = []
        children = cls.get_children(folder)
        for child in children:
            d = {'id': child.id,
                 'name': child.name,
                 'children': cls.children_dict(child)}
            children_list.append(d)
        return children_list

    @classmethod
    def get_children(cls, folder):
        """Query the children of folder"""
        if not folder:
            query = db.session.query(Dashboard) \
                .filter(
                Dashboard.type == Dashboard.data_types[1],
                ~Dashboard.path.like('%/%')
            )
        else:
            query = db.session.query(Dashboard) \
                .filter(
                Dashboard.type == Dashboard.data_types[1],
                Dashboard.path.like('{}/%'.format(folder.path)),
                ~Dashboard.path.like('{}/%/%'.format(folder.path)),
                )
        return query.all()

    @classmethod
    def get_ancestors_tree(cls, folder_ids):
        """Get thr tree which contains the paths from root to folders with ids"""
        folder_ids = [int(i) for i in folder_ids]
        bottom_folders = db.session.query(Dashboard) \
            .filter(
            Dashboard.type == Dashboard.data_types[1],
            Dashboard.id.in_(folder_ids)
        ).all()

        all_ids, routes = [], []
        for f in bottom_folders:
            route = f.path.split('/')
            route = [int(r) for r in route]
            routes.append(route)
            all_ids.extend(route)

        all_ids = set(all_ids)
        all_folders = db.session.query(Dashboard) \
            .filter(
            Dashboard.type == Dashboard.data_types[1],
            Dashboard.id.in_(all_ids)
        ).all()
        folder_names = {f.id: f.name for f in all_folders}

        tree = []
        layer_ids = [[], ]
        for route in routes:
            layer_folders = tree
            for i, folder_id in enumerate(route):
                if i >= len(layer_ids):
                    layer_ids.append([])
                if folder_id not in layer_ids[i]:
                    layer_ids[i].append(folder_id)
                    layer_folders.append({'id': folder_id,
                                          'name': folder_names.get(folder_id),
                                          'children': []})
                for d in layer_folders:
                    if d['id'] == folder_id:
                        layer_folders = d['children']
                        break
        return tree

    @classmethod
    def import_folders(cls, i_tree):
        def import_layer(i_layer, existed_layer, parent_id, folder_ids_dict):
            for i_folder in i_layer:
                for existed_folder in existed_layer:
                    if existed_folder['name'] == i_folder['name']:
                        folder_ids_dict[i_folder['id']] = existed_folder['id']
                        if i_folder['children']:
                            import_layer(i_folder['children'], existed_folder['children'],
                                         existed_folder['id'], folder_ids_dict)
                        break

                if i_folder['id'] not in folder_ids_dict.keys():
                    new_folder = cls.add_folder(i_folder['name'], parent_id)
                    folder_ids_dict[i_folder['id']] = new_folder.id
                    if i_folder['children']:
                        import_layer(i_folder['children'], [], new_folder.id,
                                     folder_ids_dict)

        folder_ids_dict = {}
        existed_tree = cls.tree_dict()
        import_layer(i_tree, existed_tree, None, folder_ids_dict)
        folder_ids_dict = {'{}'.format(k): '{}'.format(v)
                           for k, v in folder_ids_dict.items()}
        return folder_ids_dict
