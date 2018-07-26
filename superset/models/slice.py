"""A collection of ORM sqlalchemy models for Superset"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import json
import logging
from werkzeug.datastructures import ImmutableMultiDict

from flask import g, escape, Markup
from flask_babel import lazy_gettext as _
from flask_appbuilder import Model
from flask_appbuilder.models.decorators import renders
from sqlalchemy import (
    Column, Integer, String, ForeignKey, Text, Boolean, UniqueConstraint
)
from sqlalchemy.orm import relationship, backref
from sqlalchemy.orm.session import make_transient

from superset import app, db, utils
from superset.viz import viz_types
from superset.exception import ParameterException, PermissionException
from .base import AuditMixinNullable, ImportMixin
from .dataset import Dataset
from .connection import Database

config = app.config


class Slice(Model, AuditMixinNullable, ImportMixin):
    """A slice is essentially a report or a view on data"""
    __tablename__ = 'slices'
    model_type = 'slice'
    guardian_type = model_type.upper()

    id = Column(Integer, primary_key=True)
    slice_name = Column(String(128), unique=True)
    online = Column(Boolean, default=False)
    datasource_id = Column(Integer)
    datasource_type = Column(String(32))
    datasource_name = Column(String(128))
    database_id = Column(Integer, ForeignKey('dbs.id'), nullable=True)
    database = relationship('Database',
                            backref=backref('slice'),
                            foreign_keys=[database_id])
    full_table_name = Column(String(128))
    viz_type = Column(String(32))
    params = Column(Text)
    description = Column(Text)
    department = Column(String(256))
    cache_timeout = Column(Integer)

    __table_args__ = (
        UniqueConstraint('slice_name', name='slice_name_uc'),
    )

    export_fields = ('slice_name', 'online', 'datasource_id', 'datasource_type',
                     'datasource_name', 'database_id', 'full_table_name',
                     'viz_type', 'params', 'description', 'cache_timeout')

    def __repr__(self):
        return self.slice_name

    @property
    def name(self):
        return self.slice_name

    @classmethod
    def name_column(cls):
        return cls.slice_name

    @property
    def perm(self):
        return self.slice_name

    @property
    def datasource(self):
        return self.get_datasource

    @datasource.getter
    @utils.memoized
    def get_datasource(self):
        if self.database_id and self.full_table_name:
            return Dataset.temp_dataset(self.database_id, self.full_table_name)
        elif self.datasource_id:
            return db.session.query(Dataset).filter_by(id=self.datasource_id).first()
        else:
            return None

    @renders('datasource_name')
    def datasource_link(self):
        datasource = self.datasource
        if datasource:
            return self.datasource.link

    @property
    def datasource_edit_url(self):
        self.datasource.url

    @property
    @utils.memoized
    def viz(self):
        d = json.loads(self.params)
        viz_class = viz_types[self.viz_type]
        return viz_class(self.datasource, form_data=d)

    @property
    def description_markeddown(self):
        return utils.markdown(self.description)

    @property
    def data(self):
        """Data used to render slice in templates"""
        d = {}
        self.token = ''
        try:
            d = self.viz.data
            self.token = d.get('token')
        except Exception as e:
            logging.exception(e)
            d['error'] = str(e)
        d['slice_id'] = self.id
        d['slice_name'] = self.slice_name
        d['description'] = self.description
        d['slice_url'] = self.slice_url
        d['edit_url'] = self.edit_url
        d['description_markeddown'] = self.description_markeddown
        return d

    @property
    def json_data(self):
        return json.dumps(self.data)

    @property
    def slice_url(self):
        """Defines the url to access the slice"""
        try:
            slice_params = json.loads(self.params)
        except Exception as e:
            logging.exception(e)
            slice_params = {}
        slice_params['slice_id'] = self.id
        slice_params['json'] = "false"
        slice_params['slice_name'] = self.slice_name
        from werkzeug.urls import Href
        href = Href(
            "/p/explore/{obj.datasource_type}/"
            "{obj.datasource_id}/".format(obj=self))
        return href(slice_params)

    @property
    def source_table_url(self):
        if self.database_id and self.full_table_name:
            return "/p/explore/table/0/?database_id={}&full_tb_name={}" \
                .format(self.database_id, self.full_table_name)
        else:
            return None

    @property
    def slice_id_url(self):
        return (
            "/p/{slc.datasource_type}/{slc.datasource_id}/{slc.id}/"
        ).format(slc=self)

    @property
    def edit_url(self):
        return "/slice/edit/{}".format(self.id)

    @property
    def slice_link(self):
        url = self.slice_url
        name = escape(self.slice_name)
        return Markup('<a href="{url}">{name}</a>'.format(**locals()))

    def get_viz(self, url_params_multidict=None):
        """Creates :py:class:viz.BaseViz object from the url_params_multidict.

        :param werkzeug.datastructures.MultiDict url_params_multidict:
            Contains the visualization params, they override the self.params
            stored in the database
        :return: object of the 'viz_type' type that is taken from the
            url_params_multidict or self.params.
        :rtype: :py:class:viz.BaseViz
        """
        slice_params = json.loads(self.params)  # {}
        slice_params['slice_id'] = self.id
        slice_params['json'] = "false"
        slice_params['slice_name'] = self.slice_name
        slice_params['viz_type'] = self.viz_type if self.viz_type else "table"
        if url_params_multidict:
            slice_params.update(url_params_multidict)
            to_del = [k for k in slice_params if k not in url_params_multidict]
            for k in to_del:
                del slice_params[k]

        immutable_slice_params = ImmutableMultiDict(slice_params)
        return viz_types[immutable_slice_params.get('viz_type')](
            self.datasource,
            form_data=immutable_slice_params,
            slice_=self
        )

    @classmethod
    def import_obj(cls, session, i_slice, solution, grant_owner_perms):
        """Inserts or overrides slc in the database.
        """
        def link_datasource(slice, database, dataset):
            if database:
                slice.database_id = database.id
                slice.database = database
            elif dataset:
                slice.datasource_id = dataset.id
                slice.datasource_name = dataset.name
            return slice

        make_transient(i_slice)
        i_slice.id = None
        i_slice.dashboards = []
        existed_slice = cls.get_object(name=i_slice.slice_name)
        new_slice = existed_slice

        new_database, new_dataset = None, None
        if i_slice.database_id and i_slice.database:
            i_database = i_slice.database
            new_database = Database.import_obj(
                session, i_database, solution, grant_owner_perms)
        elif i_slice.datasource_name:
            new_dataset = Dataset.lookup_object(i_slice.datasource_name, solution)

        if not existed_slice:
            logging.info('Importing slice: [{}] (add)'.format(i_slice))
            new_slice = i_slice.copy()
            new_slice = link_datasource(new_slice, new_database, new_dataset)
            session.add(new_slice)
            session.commit()
            grant_owner_perms([cls.guardian_type, new_slice.slice_name])
        else:
            policy, new_name = cls.get_policy(cls.model_type, i_slice.name, solution)
            if policy == cls.Policy.OVERWRITE:
                logging.info('Importing slice: [{}] (overwrite)'.format(i_slice))
                new_slice.override(i_slice)
                new_slice = link_datasource(new_slice, new_database, new_dataset)
                session.commit()
            elif policy == cls.Policy.RENAME:
                logging.info('Importing slice: [{}] (rename to [{}])'
                             .format(i_slice, new_name))
                new_slice = i_slice.copy()
                new_slice.slice_name = new_name
                new_slice = link_datasource(new_slice, new_database, new_dataset)
                session.add(new_slice)
                session.commit()
                grant_owner_perms([cls.guardian_type, new_slice.slice_name])
            elif policy == cls.Policy.SKIP:
                logging.info('Importing slice: [{}] (skip)'.format(i_slice))

        return new_slice

    @classmethod
    def check_online(cls, slice_id, raise_if_false=True):
        def check(obj, user_id):
            user_id = int(user_id)
            if (hasattr(obj, 'online') and obj.online is True) or \
                            obj.created_by_fk == user_id:
                return True
            return False

        if not slice_id:
            logging.info("No slice_id is passed to check if slice is available")
            return True
        user_id = g.user.get_id()
        slice = db.session.query(Slice).filter_by(id=slice_id).first()
        if not slice:
            raise ParameterException(
                _("Not found slice by id [{id}]").format(id=slice_id))
        if check(slice, user_id) is False:
            if raise_if_false:
                raise PermissionException(
                    _("Slice [{slice}] is offline").format(slice=slice.slice_name))
            else:
                return False
