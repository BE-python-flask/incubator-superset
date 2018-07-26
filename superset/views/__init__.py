from flask_babel import gettext as __
from superset import appbuilder
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

appbuilder.add_view_no_menu(core.Superset)
appbuilder.add_view_no_menu(core.KV)
appbuilder.add_view_no_menu(core.R)
appbuilder.add_view(
    dashboard.DashboardModelView,
    "Dashboards",
    label=__("Dashboard"),
    icon="fa-dashboard",
    category='',
    category_icon='')
appbuilder.add_view(
    slice.SliceModelView,
    "Slices",
    label=__("Slice"),
    icon="fa-bar-chart",
    category="",
    category_icon='')

appbuilder.add_view_no_menu(connection.ConnectionView)
appbuilder.add_view_no_menu(connection.HDFSConnectionModelView)
appbuilder.add_view(
    connection.DatabaseView,
    "Databases",
    label=__("Connection"),
    icon="fa-database",
    category="Sources",
    category_label=__("Datasource"),
    category_icon='fa-database',)

appbuilder.add_view_no_menu(dataset.HDFSTableModelView)
appbuilder.add_view_no_menu(dataset.TableColumnInlineView)
appbuilder.add_view_no_menu(dataset.SqlMetricInlineView)
appbuilder.add_view(
    dataset.DatasetModelView,
    "Dataset",
    label=__("Dataset"),
    category="Sources",
    category_label=__("Datasource"),
    icon='fa-table',)

appbuilder.add_view_no_menu(sql_lab.QueryView)
appbuilder.add_view_no_menu(sql_lab.SQLLab)
appbuilder.add_link(
    'SQL Editor',
    href='/p/sqllab',
    category_icon="fa-flask",
    icon="fa-flask",
    category='SQL Lab',
    category_label=__("SQL Lab"),)
appbuilder.add_link(
    'Query Search',
    href='/p/sqllab#search',
    icon="fa-search",
    category_icon="fa-flask",
    category='SQL Lab',
    category_label=__("SQL Lab"),)

appbuilder.add_view_no_menu(hdfs.HDFSBrowser)
appbuilder.add_link(
    'HDFS Browser',
    href='/hdfs/',
    label=__("HDFS"),
    icon="fa-flask",
    category='',
    category_icon='')

appbuilder.add_view_no_menu(user.PresentUserView)
appbuilder.add_view_no_menu(user.UserView)
appbuilder.add_view_no_menu(guardian.GuardianView)
