from flask_babel import gettext as __
from superset import appbuilder
from . import annotations
from . import base
from . import home
from . import core_pilot
from . import connection
from . import dataset
from . import slice
from . import dashboard
from . import sql_lab
from . import hdfs
from . import user
from . import guardian


# appbuilder.add_link(                                        # superset menu
#     'Import Dashboards',
#     label=__('Import Dashboards'),
#     href='/superset/import_dashboards',
#     icon='fa-cloud-upload',
#     category='Manage',
#     category_label=__('Manage'),
#     category_icon='fa-wrench')
# appbuilder.add_view(                                        # superset menu
#     core_pilot.CssTemplateModelView,
#     'CSS Templates',
#     label=__('CSS Templates'),
#     icon='fa-css3',
#     category='Manage',
#     category_label=__('Manage'),
#     category_icon='')
# appbuilder.add_view(                                        # superset menu
#     annotations.AnnotationLayerModelView,
#     'Annotation Layers',
#     label=__('Annotation Layers'),
#     icon='fa-comment',
#     category='Manage',
#     category_label=__('Manage'),
#     category_icon='')
# appbuilder.add_view(                                        # superset menu
#     annotations.AnnotationModelView,
#     'Annotations',
#     label=__('Annotations'),
#     icon='fa-comments',
#     category='Manage',
#     category_label=__('Manage'),
#     category_icon='')
# appbuilder.add_view(                                        # superset menu
#     core_pilot.LogModelView,
#     'Action Log',
#     label=__('Action Log'),
#     category='Security',
#     category_label=__('Security'),
#     icon='fa-list-ol')
appbuilder.add_view_no_menu(core_pilot.CssTemplateAsyncModelView)  # superset menu


appbuilder.add_view_no_menu(core_pilot.KV)
appbuilder.add_view_no_menu(core_pilot.R)
appbuilder.add_view_no_menu(core_pilot.Superset)


appbuilder.add_view(
    home.Home,
    "Home",
    label=__("Home"),
    category='',
    category_label='',
    icon="fa-list-ol")


# appbuilder.add_view(                                            # superset menu
#     dashboard.DashboardModelView,
#     'Dashboards',
#     label=__('Dashboard_Superset'),
#     icon='fa-dashboard',
#     category='Dashboard',
#     category_icon='')
appbuilder.add_view(
    dashboard.PilotDashboardModelView,
    'Dashboards',
    label=__('Dashboard'),
    icon='fa-dashboard')
appbuilder.add_view_no_menu(dashboard.DashboardModelViewAsync)
appbuilder.add_view_no_menu(dashboard.DashboardAddView)


# appbuilder.add_view(                                            # superset menu
#     slice.SliceModelView,
#     'Slice',
#     label=__('Slice_Superset'),
#     icon='fa-bar-chart',
#     category='Slice',
#     category_icon='')
appbuilder.add_view(
    slice.PilotSliceModelView,
    'Slice',
    label=__('Slice'),
    icon='fa-bar-chart',)
appbuilder.add_view_no_menu(slice.SliceAsync)
appbuilder.add_view_no_menu(slice.SliceAddView)


appbuilder.add_view_no_menu(connection.DatabaseAsync)           # superset menu
appbuilder.add_view_no_menu(connection.CsvToDatabaseView)       # superset menu
appbuilder.add_view_no_menu(connection.DatabaseTablesAsync)     # superset menu
# appbuilder.add_view(                                            # superset menu
#     connection.DatabaseView,
#     'Databases_Superset',
#     label=__('Databases_Superset'),
#     icon='fa-database',
#     category='Sources',
#     category_label=__('Sources'),
#     category_icon='fa-database')
appbuilder.add_view_no_menu(connection.ConnectionView)
appbuilder.add_view_no_menu(connection.HDFSConnectionModelView)
appbuilder.add_view(
    connection.PilotDatabaseView,
    'Connection',
    label=__('Connection'),
    icon='fa-database',
    category='Sources',
    category_label=__('Sources'),
    category_icon='fa-database')


appbuilder.add_view_no_menu(dataset.SqlMetricInlineView)
# appbuilder.add_view_no_menu(dataset.TableModelView)             # superset menu
# appbuilder.add_link(                                            # superset menu
#     'Tables',
#     label=__('Dataset_Superset'),
#     href='/tablemodelview/list/?_flt_1_is_sqllab_view=y',
#     icon='fa-upload',
#     category='Sources',
#     category_label=__('Sources'),
#     category_icon='fa-table')
appbuilder.add_view_no_menu(dataset.HDFSTableModelView)
appbuilder.add_view_no_menu(dataset.TableColumnInlineView)
appbuilder.add_view(
    dataset.DatasetModelView,
    "Dataset",
    label=__("Dataset"),
    category="Sources",
    category_label=__("Datasource"),
    icon='fa-table',)


# appbuilder.add_link(                                            # superset menu
#     'Upload a CSV',
#     label=__('Upload a CSV'),
#     href='/csvtodatabaseview/form',
#     icon='fa-upload',
#     category='Sources',
#     category_label=__('Sources'),
#     category_icon='fa-wrench')


appbuilder.add_link(
    'SQL Editor',
    href='/superset/sqllab',
    category_icon="fa-flask",
    icon="fa-flask",
    category='SQL Lab',
    category_label=__("SQL Lab"),)
appbuilder.add_link(
    'Query Search',
    href='/superset/sqllab#search',
    icon="fa-search",
    category_icon="fa-flask",
    category='SQL Lab',
    category_label=__("SQL Lab"),)
appbuilder.add_link(
    __('Saved Queries'),
    href='/sqllab/my_queries/',
    icon='fa-save',
    category='SQL Lab')
# appbuilder.add_view(                                              # superset menu
#     sql_lab.QueryView,
#     'Queries',
#     label=__('Queries'),
#     category='Manage',
#     category_label=__('Manage'),
#     icon='fa-search')
appbuilder.add_view_no_menu(sql_lab.SqlLab)
appbuilder.add_view_no_menu(sql_lab.SavedQueryViewApi)
appbuilder.add_view_no_menu(sql_lab.SavedQueryView)


appbuilder.add_link(
    'HDFS Browser',
    href='/hdfs/',
    label=__("HDFS"),
    icon="fa-flask",
    category='',
    category_icon='')
appbuilder.add_view_no_menu(hdfs.HDFSBrowser)


appbuilder.add_view_no_menu(user.PresentUserView)
appbuilder.add_view_no_menu(user.UserView)
appbuilder.add_view_no_menu(guardian.GuardianView)
