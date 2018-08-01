import json
import re
import humanize
from datetime import datetime
from flask import escape, Markup
from flask_babel import lazy_gettext as _
from flask_appbuilder.models.mixins import AuditMixin
from flask_appbuilder.models.decorators import renders

from sqlalchemy import Column, Integer,DateTime, ForeignKey
from sqlalchemy.ext.declarative import declared_attr

from superset import app, db, sm
from superset.exception import ParameterException, PropertyException
from superset.utils import QueryStatus
from superset.message import NAME_RESTRICT_ERROR


config = app.config


def json_to_dict(json_str):
    if json_str:
        val = re.sub(',[ \t\r\n]+}', '}', json_str)
        val = re.sub(',[ \t\r\n]+\]', ']', val)
        return json.loads(val)
    else:
        return {}


class QueryResult(object):
    """Object returned by the query interface"""
    def __init__(  # noqa
            self,
            df,
            query,
            duration,
            status=QueryStatus.SUCCESS,
            error_message=None):
        self.df = df
        self.query = query
        self.duration = duration
        self.status = status
        self.error_message = error_message


class ValueRestrict(object):
    NAME_RESTRICT_PATTERN = '^(?!_)(?!.*?_$)[a-zA-Z0-9_\u4e00-\u9fa5]+$'

    @classmethod
    def check_name(cls, name):
        match = re.search(cls.NAME_RESTRICT_PATTERN, name)
        if not match:
            raise PropertyException(NAME_RESTRICT_ERROR)


class ImportMixin(ValueRestrict):

    class Policy(object):
        """
        When import, the objects could have same names between objects in file
        and in database. So need user give a 'solution':
         {"database": {"name1": {"policy": "skip", "new_name": null}, {...}},
         "dataset": {...}}
        """
        SKIP = 'skip'
        OVERWRITE = 'overwrite'
        RENAME = 'rename'
        POLICY_DICT = {'SKIP': SKIP, 'OVERWRITE': OVERWRITE, 'RENAME': RENAME}

    def override(self, obj):
        """Overrides the plain fields of the dashboard."""
        for field in obj.__class__.export_fields:
            setattr(self, field, getattr(obj, field))

    def copy(self):
        """Creates a copy of the dashboard without relationships."""
        new_obj = self.__class__()
        new_obj.override(self)
        return new_obj

    def alter_params(self, **kwargs):
        d = self.params_dict
        d.update(kwargs)
        self.params = json.dumps(d)

    @property
    def params_dict(self):
        if self.params:
            params = re.sub(",[ \t\r\n]+}", "}", self.params)
            params = re.sub(",[ \t\r\n]+\]", "]", params)
            return json.loads(params)
        else:
            return {}

    @classmethod
    def get_policy(cls, obj_type, obj_name, solution):
        policy, new_name = cls.Policy.SKIP, None
        if solution.get(obj_type) and solution.get(obj_type).get(obj_name):
            policy = solution[obj_type][obj_name]['policy']
            if policy == cls.Policy.RENAME:
                new_name = solution[obj_type][obj_name]['new_name']
        return policy, new_name

    @classmethod
    def lookup_object(cls, obj_name, solution):
        policy, new_name = cls.get_policy(cls.model_type, obj_name, solution)
        obj_name = new_name if new_name else obj_name
        return cls.get_object(name=obj_name)

    @classmethod
    def check_solution(cls, solution, session, models_dict):
        """
        :param solution: The soultion to solve the problem of same names between
        objects in imported file and in database. It's a dict:
        {"database": {"name1": {"policy": "skip", "new_name": None},
                      "name2": {"policy": "rename", "new_name": "name22"},...}
         "dataset": {...},...}
        :return:
        """
        new_names = {}
        for obj_type, sames in solution.items():
            new_names[obj_type] = []
            for name, policy_dict in sames.items():
                policy = policy_dict.get('policy')
                new_name = policy_dict.get('new_name')
                if policy not in cls.Policy.POLICY_DICT.values():
                    raise ParameterException(
                        _('Error policy string: [{policy}]').format(policy=policy))
                if policy == cls.Policy.RENAME:
                    if not new_name:
                        raise ParameterException(
                            _('Want to rename [{name}], but new name is None')
                                .format(name=name))
                    match = re.search(cls.NAME_RESTRICT_PATTERN, new_name)
                    if not match:
                        raise ParameterException(NAME_RESTRICT_ERROR)
                    if new_name in new_names[obj_type]:
                        raise ParameterException(
                            _('Duplicated {obj_type} new name [{name}]')
                                .format(obj_type=obj_type, name=name))
                    new_names[obj_type].append(new_name)

        for obj_type, names in new_names.items():
            model = models_dict[obj_type]
            if names:
                objs = session.query(model).filter(model.name_column().in_(names)).all()
                if objs:
                    names_str = [o.name for o in objs]
                    raise ParameterException(
                        _('The {obj_type} new name {name} is existed')
                            .format(obj_type=obj_type, name=names_str))


class AuditMixinNullable(AuditMixin):
    """Altering the AuditMixin to use nullable fields

    Allows creating objects programmatically outside of CRUD
    """
    created_on = Column(DateTime, default=datetime.now, nullable=True)
    changed_on = Column(DateTime, default=datetime.now,
                        onupdate=datetime.now, nullable=True)

    def guardian_datasource(self):
        return [self.model_type.upper(), self.name]

    @declared_attr
    def created_by_fk(cls):  # noqa
        return Column(Integer, ForeignKey('ab_user.id'),
                      default=cls.get_user_id, nullable=True)

    @declared_attr
    def changed_by_fk(cls):  # noqa
        return Column(
            Integer, ForeignKey('ab_user.id'),
            default=cls.get_user_id, onupdate=cls.get_user_id, nullable=True)

    def _user_link(self, user):
        if not user:
            return ''
        url = '/superset/profile/{}/'.format(user.username)
        return Markup('<a href="{}">{}</a>'.format(url, escape(user) or ''))

    @renders('created_by')
    def creator(self):  # noqa
        return self._user_link(self.created_by)

    @property
    def changed_by_(self):
        return self._user_link(self.changed_by)

    @renders('changed_on')
    def changed_on_(self):
        return Markup(
            '<span class="no-wrap">{}</span>'.format(self.changed_on))

    @renders('changed_on')
    def modified(self):
        s = humanize.naturaltime(datetime.now() - self.changed_on)
        return Markup('<span class="no-wrap">{}</span>'.format(s))

    @property
    def icons(self):
        return """
        <a
                href="{self.datasource_edit_url}"
                data-toggle="tooltip"
                title="{self.datasource}">
            <i class="fa fa-database"></i>
        </a>
        """.format(**locals())

    @classmethod
    def count(cls):
        return db.session.query(cls).count()

    @classmethod
    def get_object(cls, id=None, name=None):
        if id:
            return db.session.query(cls).filter_by(id=id).first()
        else:
            return db.session.query(cls).filter(cls.name_column() == name).first()


class Queryable(object):

    """A common interface to objects that are queryable (tables and datasources)"""

    @property
    def column_names(self):
        return sorted([c.column_name for c in self.columns])

    @property
    def main_dttm_col(self):
        return "timestamp"

    @property
    def groupby_column_names(self):
        return sorted([c.column_name for c in self.columns if c.groupby])

    @property
    def filterable_column_names(self):
        return sorted([c.column_name for c in self.columns if c.filterable])

    @property
    def dttm_cols(self):
        return []

    @property
    def url(self):
        return '/{}/edit/{}'.format(self.baselink, self.id)

    @property
    def explore_url(self):
        if self.default_endpoint:
            return self.default_endpoint
        else:
            return "/superset/explore/{obj.type}/{obj.id}/" \
                   "?datasource_id={obj.id}&datasource_type=table"\
                .format(obj=self)

    @property
    def data(self):
        """data representation of the datasource sent to the frontend"""
        gb_cols = [(col, col) for col in self.groupby_column_names]
        all_cols = [(c, c) for c in self.column_names]
        order_by_choices = []
        for s in sorted(self.column_names):
            order_by_choices.append((json.dumps([s, True]), s + ' [asc]'))
            order_by_choices.append((json.dumps([s, False]), s + ' [desc]'))

        d = {
            'id': self.id,
            'type': self.type,
            'name': self.name,
            'metrics_combo': self.metrics_combo,
            'order_by_choices': order_by_choices,
            'gb_cols': gb_cols,
            'all_cols': all_cols,
            'filterable_cols': self.filterable_column_names,
        }
        if (self.type == 'table'):
            grains = self.database.grains() or []
            if grains:
                grains = [(g.name, g.name) for g in grains]
            d['granularity_sqla'] = [(c, c) for c in self.dttm_cols]
            d['time_grain_sqla'] = grains
        return d


def merge_perm(sm, permission_name, view_menu_name, connection):

    permission = sm.find_permission(permission_name)
    view_menu = sm.find_view_menu(view_menu_name)
    pv = None

    if not permission:
        permission_table = sm.permission_model.__table__
        connection.execute(
            permission_table.insert()
                .values(name=permission_name),
                )
    if not view_menu:
        view_menu_table = sm.viewmenu_model.__table__
        connection.execute(
            view_menu_table.insert()
                .values(name=view_menu_name),
                )

    permission = sm.find_permission(permission_name)
    view_menu = sm.find_view_menu(view_menu_name)

    if permission and view_menu:
        pv = sm.get_session.query(sm.permissionview_model).filter_by(
            permission=permission, view_menu=view_menu).first()
    if not pv and permission and view_menu:
        permission_view_table = sm.permissionview_model.__table__
        connection.execute(
            permission_view_table.insert()
                .values(
                permission_id=permission.id,
                view_menu_id=view_menu.id,
            ),
                )


def set_perm(mapper, connection, target):  # noqa

    if target.perm != target.get_perm():
        link_table = target.__table__
        connection.execute(
            link_table.update()
                .where(link_table.c.id == target.id)
                .values(perm=target.get_perm()),
                )

    # add to view menu if not already exists
    merge_perm(sm, 'datasource_access', target.get_perm(), connection)
