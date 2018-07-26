from flask_babel import lazy_gettext as _

ALL_DATASOURCE_ACCESS_ERR = _(
    "This endpoint requires the `all_datasource_access` permission")
DATASOURCE_MISSING_ERR = _("The datasource seems to have been deleted")
DATASOURCE_ACCESS_ERR = _("You don't have access to this datasource")

OBJECT_NOT_FOUND = _("Not found this object")
NO_USER = _("Can't get user's information. Maybe session is expired. Try login again.")

ONLINE_SUCCESS = _("Success to be online")
OFFLINE_SUCCESS = _("Success to be offline")
OBJECT_IS_ONLINE = _("This object is already online")
OBJECT_IS_OFFLINE = _("This object is already offline")
NO_ONLINE_PERMISSION = _("No permission for 'online' and 'offline'")

ADD_SUCCESS = _("Success to add")
ADD_FAILED = _("Failed to add, may cased by unique restriction")
UPDATE_SUCCESS = _("Success to update")
UPDATE_FAILED = _("Failed to update, may cased by unique restriction")
DELETE_SUCCESS = _("Success to delete")
DELETE_FAILED = _("Failed to delete")

DUPLICATE_NAME = _("Duplicate name")

MISS_PASSWORD_FOR_GUARDIAN = _("Miss password to access Guardian")

COLUMN_MISS_DATASET = _("Miss parameter [dataset_id] to query columns")
METRIC_MISS_DATASET = _("Miss parameter [dataset_id] to query metrics")

# filerobot
LOGIN_FILEROBOT_FAILED = _("Failed to login FileRobot")
LOGIN_FILEROBOT_SUCCESS = _("Success to login FileRobot")
LOGOUT_FILEROBOT_SUCCESS = _("Success to logout FileRobot")
MISS_PASSWORD_FOR_FILEROBOT = _("Miss password to login FileRobot")
NO_HDFS_CONNECTION = _("No HDFS connection, please create one")
NO_FILEROBOT_SERVER = _("Failed to get FileRobot server from config")

# views/base.py
NAME_RESTRICT_ERROR = _("Name should consist of chinese, alphanumeric characters "
                        "and underscores, and can't start or end with underscores")
FOLDER_END_ERROR = _("Folder path should be not end with '/'")

# views/connection.py
NONE_CONNECTION_NAME = _("Connection name can't be none")
NONE_CONNECTION_TYPE = _("Connection type can't be none")
NONE_SQLALCHEMY_URI = _("Sqlalchemy uri can't be none")
NONE_CONNECTION_ARGS = _("Connection args can't be none")
NONE_HTTPFS = _("Httpfs can't be none")
CANNOT_EDIT_DEFAULT_CONN = _("Can't edit default connection")

# views/dataset.py
NONE_COLUMN_NAME = _("Column name can't be none")
NONE_METRIC_NAME = _("Metric name can't be none")
NONE_METRIC_EXPRESSION = _("Metric expression can't be none")
NONE_DATASET_NAME = _("Dataset name can't be none")
NONE_DATASET_TYPE = _("Dataset type can't be none")
NONE_CONNECTION = _("Connection can't be none")
NONE_HDFS_PATH = _("HDFS path can't be none")
NONE_HDFS_CONNECTION = _("HDFS connection can't be none")

# views/core.py
NONE_SLICE_NAME = _("Slice name can't be none")
NONE_DASHBOARD_NAME = _("Dashboard name can't be none")

CONNECTION_TIMEOUT = _("Connecting timeout")
NO_USEABLE_DATASETS = _("No useable datasets")

MOVE_DASHBOARD_SUCCESS = _("Success to move dashboard")
MOVE_FOLDER_SUCCESS = _("Success to move folder")
MOVE_FOLDER_TO_CHILD_ERROR = _('Cannot move a folder to its child folder')
NO_PERM_EDIT_ROOT_FOLDER = _('No privilege to edit root path')


# views/gaurdian.py
DISABLE_GAURDIAN = _('Not enable Guardian authentication')

# models.py/connection
DISABLE_GUARDIAN_FOR_KEYTAB = _('Not enable Guardian, can not download keytab file')
DISABLE_CAS = _('Not enable CAS authentication')
