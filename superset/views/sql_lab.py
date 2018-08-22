# -*- coding: utf-8 -*-
# pylint: disable=C,R,W
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import logging
from flask import g, redirect
from flask_babel import lazy_gettext as _
from flask_appbuilder import expose
from flask_appbuilder.models.sqla.interface import SQLAInterface

from superset import sql_lab, appbuilder
from superset.models import Database, Query, SavedQuery
from .base import SupersetModelView, BaseSupersetView, DeleteMixin, catch_exception, json_response


class QueryView(SupersetModelView):
    datamodel = SQLAInterface(Query)
    list_columns = ['user', 'database', 'status', 'start_time', 'end_time']
    label_columns = {
        'user': _('User'),
        'database': _('Database'),
        'status': _('Status'),
        'start_time': _('Start Time'),
        'end_time': _('End Time'),
    }


# appbuilder.add_view(
#     QueryView,
#     'Queries',
#     label=__('Queries'),
#     category='Manage',
#     category_label=__('Manage'),
#     icon='fa-search')


class SavedQueryView(SupersetModelView, DeleteMixin):
    datamodel = SQLAInterface(SavedQuery)

    list_title = _('List Saved Query')
    show_title = _('Show Saved Query')
    add_title = _('Add Saved Query')
    edit_title = _('Edit Saved Query')

    list_columns = [
        'label', 'user', 'database', 'schema', 'description',
        'modified', 'pop_tab_link']
    show_columns = [
        'id', 'label', 'user', 'database',
        'description', 'sql', 'pop_tab_link']
    search_columns = ('label', 'user', 'database', 'schema', 'changed_on')
    add_columns = ['label', 'database', 'description', 'sql']
    edit_columns = add_columns
    base_order = ('changed_on', 'desc')
    label_columns = {
        'label': _('Label'),
        'user': _('User'),
        'database': _('Database'),
        'description': _('Description'),
        'modified': _('Modified'),
        'end_time': _('End Time'),
        'pop_tab_link': _('Pop Tab Link'),
        'changed_on': _('Changed on'),
    }

    def pre_add(self, obj):
        obj.user = g.user

    def pre_update(self, obj):
        self.pre_add(obj)


class SavedQueryViewApi(SavedQueryView):
    show_columns = ['label', 'db_id', 'schema', 'description', 'sql']
    add_columns = show_columns
    edit_columns = add_columns


# appbuilder.add_view_no_menu(SavedQueryViewApi)
# appbuilder.add_view_no_menu(SavedQueryView)
#
# appbuilder.add_link(
#     __('Saved Queries'),
#     href='/sqllab/my_queries/',
#     icon='fa-save',
#     category='SQL Lab')


class SqlLab(BaseSupersetView):

    @expose('/my_queries/')
    def my_queries(self):
        """Assigns a list of found users to the given role."""
        return redirect(
            '/savedqueryview/list/?_flt_0_user={}'.format(g.user.id))

    @catch_exception
    @expose("/metadata/<database_id>/<schema>/")
    def extra_schema_metadata(self, database_id, schema):
        database = Database.get_object(database_id)
        metadata = database.db_engine_spec.extra_schema_metadata(database, schema)
        return json_response(data=metadata)

    @catch_exception
    @expose("/metadata/<database_id>/<schema>/<table_name>/")
    def extra_table_metadata(self, database_id, schema, table_name):
        database = Database.get_object(database_id)
        metadata = database.db_engine_spec.extra_table_metadata(
            database, schema, table_name)
        return json_response(data=metadata)

    @catch_exception
    @expose("/metadata/<database_id>/<schema>/<table_name>/<column_name>/")
    def extra_column_metadata(self, database_id, schema, table_name, column_name):
        database = Database.get_object(database_id)
        metadata = database.db_engine_spec.extra_column_metadata(
            database, schema, table_name, column_name)
        if not metadata:
            logging.error('The column [{}] is not existed in [].[]'
                          .format(column_name, schema, table_name))
        return json_response(data=metadata)

    @catch_exception
    @expose("/preview/<database_id>/<schema>/<table_name>/")
    def preview_table(self, database_id, schema, table_name):
        database = Database.get_object(database_id)
        sql = database.select_star(table_name, schema=schema, limit=100)
        payload = sql_lab.execute_sql(database_id, sql, schema=schema)
        return json_response(data=payload)

    @catch_exception
    @expose("/preview/<database_id>/<schema>/<table_name>/<column_name>/")
    def preview_column(self, database_id, schema, table_name, column_name):
        database = Database.get_object(database_id)
        sql = database.select_star(table_name, schema=schema, limit=100,
                                  columns=[column_name, ])
        payload = sql_lab.execute_sql(database_id, sql, schema=schema)
        return json_response(data=payload)

# appbuilder.add_view_no_menu(SqlLab)