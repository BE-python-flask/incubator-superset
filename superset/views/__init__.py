from flask_babel import gettext as __
from superset import appbuilder, app
from . import base
from . import home
from . import core
from . import connection
from . import dataset
from . import slice
from . import dashboard
from . import sql_lab
from . import hdfs
from . import user
from . import guardian

appbuilder.add_view(
    home.Home,
    "Home",
    label=__("Home"),
    category='',
    category_label='',
    icon="fa-list-ol")

# appbuilder.add_view_no_menu(core.Superset)
# appbuilder.add_view_no_menu(core.KV)
# appbuilder.add_view_no_menu(core.R)
# appbuilder.add_view_no_menu(core.CssTemplateModelView)
# appbuilder.add_view_no_menu(core.CssTemplateAsyncModelView)
#
# appbuilder.add_view(
#     dashboard.DashboardModelView,
#     "Dashboards",
#     label=__("Dashboard"),
#     icon="fa-dashboard",
#     category='',
#     category_icon='')
# appbuilder.add_view_no_menu(dashboard.DashboardModelViewAsync)
# appbuilder.add_view_no_menu(dashboard.DashboardAddView)

# appbuilder.add_view(
#     slice.SliceModelView,
#     "Slices",
#     label=__("Slice"),
#     icon="fa-bar-chart",
#     category="",
#     category_icon='')
# appbuilder.add_view_no_menu(slice.SliceAsync)
# appbuilder.add_view_no_menu(slice.SliceAddView)

# appbuilder.add_view_no_menu(connection.ConnectionView)
# appbuilder.add_view_no_menu(connection.HDFSConnectionModelView)
# appbuilder.add_view(
#     connection.DatabaseView,
#     "Databases",
#     label=__("Connection"),
#     icon="fa-database",
#     category="Sources",
#     category_label=__("Datasource"),
#     category_icon='fa-database',)

# appbuilder.add_view_no_menu(dataset.HDFSTableModelView)
# appbuilder.add_view_no_menu(dataset.TableColumnInlineView)
# appbuilder.add_view_no_menu(dataset.SqlMetricInlineView)
# appbuilder.add_view(
#     dataset.DatasetModelView,
#     "Dataset",
#     label=__("Dataset"),
#     category="Sources",
#     category_label=__("Datasource"),
#     icon='fa-table',)

appbuilder.add_view_no_menu(sql_lab.SQLLab)
appbuilder.add_view_no_menu(sql_lab.SavedQueryViewApi)
appbuilder.add_view_no_menu(sql_lab.SavedQueryView)
appbuilder.add_view_no_menu(sql_lab.QueryView)
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


appbuilder.add_view_no_menu(hdfs.HDFSBrowser)
appbuilder.add_link(
    'HDFS Browser',
    href='/hdfs/',
    label=__("HDFS"),
    icon="fa-flask",
    category='',
    category_icon='')

# appbuilder.add_view_no_menu(user.PresentUserView)
# appbuilder.add_view_no_menu(user.UserView)
# appbuilder.add_view_no_menu(guardian.GuardianView)


#######################################
# superset menu
#######################################

appbuilder.add_link(
    'Import Dashboards',
    label=__('Import Dashboards'),
    href='/superset/import_dashboards',
    icon='fa-cloud-upload',
    category='Manage',
    category_label=__('Manage'),
    category_icon='fa-wrench')


appbuilder.add_view(
    connection.DatabaseView1,
    'Databases_Superset',
    label=__('Databases_Superset'),
    icon='fa-database',
    category='Sources',
    category_label=__('Sources'),
    category_icon='fa-database')


appbuilder.add_view(
    connection.DatabaseView2,
    'Databases_Pilot',
    label=__('Databases_Pilot'),
    icon='fa-database',
    category='Sources',
    category_label=__('Sources'),
    category_icon='fa-database')

appbuilder.add_view_no_menu(connection.DatabaseAsync)
appbuilder.add_view_no_menu(connection.CsvToDatabaseView)
appbuilder.add_view_no_menu(connection.DatabaseTablesAsync)


appbuilder.add_view(
    slice.SliceModelView,
    'Slice',
    label=__('Slice'),
    icon='fa-bar-chart',
    category='',
    category_icon='')
appbuilder.add_view_no_menu(slice.SliceAsync)
appbuilder.add_view_no_menu(slice.SliceAddView)


appbuilder.add_view(
    dashboard.DashboardModelView,
    'Dashboards',
    label=__('Dashboard'),
    icon='fa-dashboard',
    category='',
    category_icon='')
appbuilder.add_view_no_menu(dashboard.DashboardModelViewAsync)
appbuilder.add_view_no_menu(dashboard.DashboardAddView)


appbuilder.add_view(
    core.LogModelView,
    'Action Log',
    label=__('Action Log'),
    category='Security',
    category_label=__('Security'),
    icon='fa-list-ol')

appbuilder.add_view_no_menu(core.KV)
appbuilder.add_view_no_menu(core.R)
appbuilder.add_view_no_menu(core.Superset)


appbuilder.add_separator('Sources')
appbuilder.add_view(
    core.CssTemplateModelView,
    'CSS Templates',
    label=__('CSS Templates'),
    icon='fa-css3',
    category='Manage',
    category_label=__('Manage'),
    category_icon='')

appbuilder.add_view_no_menu(core.CssTemplateAsyncModelView)

# appbuilder.add_link(
#     'SQL Editor',
#     label=__('SQL Editor'),
#     href='/superset/sqllab',
#     category_icon='fa-flask',
#     icon='fa-flask',
#     category='SQL Lab',
#     category_label=__('SQL Lab'),
# )
#
# appbuilder.add_link(
#     'Query Search',
#     label=__('Query Search'),
#     href='/superset/sqllab#search',
#     icon='fa-search',
#     category_icon='fa-flask',
#     category='SQL Lab',
#     category_label=__('SQL Lab'),
# )

appbuilder.add_link(
    'Upload a CSV',
    label=__('Upload a CSV'),
    href='/csvtodatabaseview/form',
    icon='fa-upload',
    category='Sources',
    category_label=__('Sources'),
    category_icon='fa-wrench')
appbuilder.add_separator('Sources')


# appbuilder.add_view(
#     sql_lab.QueryView,
#     'Queries',
#     label=__('Queries'),
#     category='Manage',
#     category_label=__('Manage'),
#     icon='fa-search')
#
# appbuilder.add_view_no_menu(sql_lab.SavedQueryViewApi)
# appbuilder.add_view_no_menu(sql_lab.SavedQueryView)
# appbuilder.add_view_no_menu(sql_lab.SQLLab)
# appbuilder.add_link(
#     __('Saved Queries'),
#     href='/sqllab/my_queries/',
#     icon='fa-save',
#     category='SQL Lab')
