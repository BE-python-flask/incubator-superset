from datetime import timedelta, date
import logging
import json
import re
import copy
from werkzeug.urls import Href
from flask import g, request, redirect
from flask_babel import lazy_gettext as _
from flask_appbuilder import expose
from flask_appbuilder.security.sqla.models import User
from sqlalchemy import func, and_

from superset import appbuilder, db, app
from superset.models import (
    Slice, Dashboard, Database, HDFSConnection, FavStar, Log, str_to_model, Number
)
from superset.exception import ParameterException
from .base import BaseSupersetView, PermissionManagement, catch_exception, json_response

config = app.config


class Home(BaseSupersetView, PermissionManagement):
    """The api for the home page

    limit = 0: means not limit
    default_types['actions'] could be: ['online', 'offline', 'add', 'update', 'delete'...]
    """
    default_view = 'home'
    route_base = '/home'

    page = 0
    page_size = 10
    order_column = 'time'
    order_direction = 'desc'
    default_types = {
        'counts': ['dashboard', 'slice', 'dataset', 'connection'],
        'trends': ['dashboard', 'slice', 'dataset', 'connection'],
        'favorits': ['dashboard', 'slice'],
        'edits': ['dashboard', 'slice'],
        'actions': ['grant', 'revoke', 'add', 'delete']
    }
    default_limit = {
        'trends': 30,
        'favorits': 10,
        'refers': 10,
        'edits': 10,
        'actions': 10
    }
    str_to_column_in_actions = {
        'user': User.username,
        'action': Log.action,
        'time': Log.dttm
    }
    str_to_column_in_edits = {
        'name': {'slice': Slice.slice_name, 'dashboard': Dashboard.name},
        'time': {'slice': Slice.changed_on, 'dashboard': Dashboard.changed_on}
    }

    def __init__(self):
        super(Home, self).__init__()
        self.status = 201
        self.success = True
        self.message = []
        self.global_read = True

    def get_obj_class(self, type_):
        try:
            model = str_to_model[type_.lower()]
        except KeyError:
            self.status = 400 if str(self.status)[0] < '4' else self.status
            self.message.append(_("Error model type: [{type_}]").format(type_=type_))
            return False, None
        else:
            return True, model

    def get_object_counts(self, username, types):
        dt = {}
        for type_ in types:
            if self.global_read:
                dt[type_] = str_to_model[type_].count()
            else:
                from superset.guardian import guardian_client as client
                if type_.lower() == 'connection':
                    names = client.search_model_perms(username, Database.guardian_type) + \
                            client.search_model_perms(username, HDFSConnection.guardian_type)
                else:
                    names = client.search_model_perms(username, type_.upper())
                dt[type_] = len(names)
        return dt

    def get_object_number_trends(self, username, types, today_counts, limit=30):
        trends = {}
        today = date.today()
        start_date = date.today() - timedelta(days=limit-1)

        for type_ in types:
            present_count = today_counts.get(type_, 0)
            trends[type_] = [{"date": str(today), "count": present_count}]
            present_date = date.today() - timedelta(days=1)

            query = db.session.query(Number)\
                .filter(Number.username == username, Number.obj_type == type_)\
                .order_by(Number.dt.desc()).limit(limit)

            for number in query.all():
                if number.dt > present_date:
                    continue
                elif number.dt < present_date:
                    while number.dt < present_date:
                        if present_date < start_date:
                            break
                        trends[type_].insert(0, {"date": str(present_date),
                                                 "count": present_count})
                        present_date = present_date - timedelta(days=1)

                if present_date < start_date:
                    break
                trends[type_].insert(0, {"date": str(present_date),
                                         "count": number.count})
                present_date = present_date - timedelta(days=1)
                present_count = number.count

            while present_date >= start_date:
                trends[type_].insert(0, {"date": str(present_date),
                                         "count": present_count})
                present_date = present_date - timedelta(days=1)

        return trends

    def get_slice_types(self, limit=10):
        """Query the viz_type of slices"""
        rs = (
            db.session.query(func.count(Slice.viz_type), Slice.viz_type)
                .group_by(Slice.viz_type)
                .order_by(func.count(Slice.viz_type).desc())
                .limit(limit)
                .all()
        )
        return rs

    def get_fav_dashboards(self, username, limit=10):
        """Query the times of dashboard liked by users"""
        query = db.session.query(func.count(FavStar.obj_id), Dashboard) \
            .filter(
            and_(FavStar.class_name.ilike('dashboard'),
                 FavStar.obj_id == Dashboard.id)
        )
        query = query.group_by(FavStar.obj_id) \
            .order_by(func.count(FavStar.obj_id).desc())
        rs = query.all()

        readable_dashs = []
        if not self.global_read:
            from superset.guardian import guardian_client as client
            readable_dashs = client.search_model_perms(username, Dashboard.guardian_type)

        rows = []
        index = 0
        for count, dash in rs:
            if index >= limit:
                break
            if not self.global_read:
                if dash.name not in readable_dashs:
                    continue
            rows.append({'name': dash.name, 'link': dash.url, 'count': count})
            index += 1
        return rows

    def get_fav_slices(self, username, limit=10):
        """Query the times of slice liked by users"""
        query = db.session.query(func.count(FavStar.obj_id), Slice) \
            .filter(
            and_(FavStar.class_name.ilike('slice'),
                 FavStar.obj_id == Slice.id)
        )
        query = query.group_by(FavStar.obj_id) \
            .order_by(func.count(FavStar.obj_id).desc())
        rs = query.all()

        readable_slices = []
        if not self.global_read:
            from superset.guardian import guardian_client as client
            readable_slices = client.search_model_perms(username, Slice.guardian_type)

        rows = []
        index = 0
        for count, slice in rs:
            if index >= limit:
                break
            if not self.global_read:
                if slice.name not in readable_slices:
                    continue
            rows.append({'name': slice.name, 'link': slice.slice_url, 'count': count})
            index += 1
        return rows

    def get_fav_objects(self, username, types, limit):
        dt = {}
        if 'dashboard' in types:
            dt['dashboard'] = self.get_fav_dashboards(username, limit=limit)
        if 'slice' in types:
            dt['slice'] = self.get_fav_slices(username, limit=limit)
        return dt

    def get_refered_slices(self, username, limit=10):
        """Query the times of slice used by dashboards"""
        sql = """
            SELECT slices.slice_name, count(slices.slice_name), slices.params, 
                   slices.id, slices.datasource_id
            FROM slices, dashboards, dashboard_slices
            WHERE slices.id = dashboard_slices.slice_id
            AND dashboards.id = dashboard_slices.dashboard_id
            GROUP BY slices.slice_name, slices.params, slices.id, slices.datasource_id
            ORDER BY count(slices.slice_name) DESC
            """
        rs = db.session.execute(sql)

        readable_slices = []
        if not self.global_read:
            from superset.guardian import guardian_client as client
            readable_slices = client.search_model_perms(username, Slice.guardian_type)

        rows = []
        index = 0
        for row in rs:
            if index >= limit:
                break
            if not self.global_read:
                if row[0] not in readable_slices:
                    continue
            try:
                slice_params = json.loads(row[2])
            except Exception as e:
                logging.exception(e)
                slice_params = {}
            slice_params['slice_id'] = row[3]
            slice_params['json'] = "false"
            slice_params['slice_name'] = row[0]
            href = Href("/p/explore/table/{}/".format(row[4]))
            rows.append({'name': row[0], 'count': row[1], 'link': href(slice_params)})
            index += 1
        return rows

    def get_edited_slices(self, **kwargs):
        """The records of slice be modified"""
        username = kwargs.get('username')
        page = kwargs.get('page', 0)
        page_size = kwargs.get('page_size')
        order_column = kwargs.get('order_column')
        order_direction = kwargs.get('order_direction')

        query = db.session.query(Slice)
        if order_column:
            column = self.str_to_column_in_edits.get(order_column).get('slice')
            if order_direction == 'desc':
                query = query.order_by(column.desc())
            else:
                query = query.order_by(column)
        else:
            query = query.order_by(Slice.changed_on.desc())

        readable_slices = []
        if not self.global_read:
            from superset.guardian import guardian_client as client
            readable_slices = client.search_model_perms(username, Slice.guardian_type)
            count = len(readable_slices)
        else:
            count = query.count()
            if page_size and page_size > 0:
                query = query.limit(page_size)
            if page and page > 0:
                query = query.offset(page * page_size)

        rows = []
        index = 0
        for obj in query.all():
            if not self.global_read:
                if obj.name in readable_slices:
                    index += 1
                    if index <= page * page_size:
                        continue
                    elif index > (page+1) * page_size:
                        break
                else:
                    continue
            action = 'create' if obj.changed_on == obj.created_on else 'edit'
            line = {'name': obj.slice_name,
                    'description': obj.description,
                    'action': action,
                    'time': str(obj.changed_on),
                    'link': obj.slice_url}
            rows.append(line)
        return count, rows

    def get_edited_dashboards(self, **kwargs):
        """The records of slice be modified"""
        username = kwargs.get('username')
        page = kwargs.get('page', 0)
        page_size = kwargs.get('page_size')
        order_column = kwargs.get('order_column')
        order_direction = kwargs.get('order_direction')

        query = db.session.query(Dashboard)
        if order_column:
            column = self.str_to_column_in_edits.get(order_column).get('dashboard')
            if order_direction == 'desc':
                query = query.order_by(column.desc())
            else:
                query = query.order_by(column)
        else:
            query = query.order_by(Dashboard.changed_on.desc())

        readable_dashs = []
        if not self.global_read:
            from superset.guardian import guardian_client as client
            readable_dashs = client.search_model_perms(username, Dashboard.guardian_type)
            count = len(readable_dashs)
        else:
            count = query.count()
            if page_size and page_size > 0:
                query = query.limit(page_size)
            if page and page > 0:
                query = query.offset(page * page_size)

        rows = []
        index = 0
        for obj in query.all():
            if not self.global_read:
                if obj.name in readable_dashs:
                    index += 1
                    if index <= page * page_size:
                        continue
                    elif index > (page+1) * page_size:
                        break
                else:
                    continue
            action = 'create' if obj.changed_on == obj.created_on else 'edit'
            line = {'name': obj.name,
                    'description': obj.description,
                    'action': action,
                    'time': str(obj.changed_on),
                    'link': obj.url}
            rows.append(line)
        return count, rows

    def get_edited_objects(self, **kwargs):
        dt = {}
        types = kwargs.pop('types')
        if 'slice' in types:
            count, dt['slice'] = self.get_edited_slices(**kwargs)
        if 'dashboard' in types:
            count, dt['dashboard'] = self.get_edited_dashboards(**kwargs)
        return dt

    def get_request_args(self, args):
        kwargs = {}
        kwargs['page'] = int(args.get('page', self.page))
        kwargs['page_size'] = int(args.get('page_size', self.page_size))
        kwargs['order_column'] = args.get('order_column', self.order_column)
        kwargs['order_direction'] = args.get('order_direction', self.order_direction)
        return kwargs

    @catch_exception
    @expose('/edits/slice/')
    def get_edited_slices_by_url(self):
        self.set_global_read()
        kwargs = self.get_request_args(request.args)
        kwargs['username'] = g.user.username
        count, data = self.get_edited_slices(**kwargs)

        status_ = self.status
        message_ = self.message
        self.status = 200
        self.message = []
        if str(self.status)[0] != '2':
            return json_response(message=message_, status=status_, code=1),
        else:
            response = {}
            response['data'] = data
            response['count'] = count
            response['page'] = kwargs.get('page')
            response['page_size'] = kwargs.get('page_size')
            response['order_column'] = kwargs.get('order_column')
            response['order_direction'] = kwargs.get('order_direction')
            return json_response(data=response)

    @catch_exception
    @expose('/edits/dashboard/')
    def get_edited_dashboards_by_url(self):
        self.set_global_read()
        kwargs = self.get_request_args(request.args)
        kwargs['username'] = g.user.username
        count, data = self.get_edited_dashboards(**kwargs)

        status_ = self.status
        message_ = self.message
        self.status = 200
        self.message = []
        if str(self.status)[0] != '2':
            return json_response(message=message_, status=status_, code=1)
        else:
            response = {}
            response['data'] = data
            response['count'] = count
            response['page'] = kwargs.get('page')
            response['page_size'] = kwargs.get('page_size')
            response['order_column'] = kwargs.get('order_column')
            response['order_direction'] = kwargs.get('order_direction')
            return json_response(data=response)

    def get_user_actions(self, **kwargs):
        """The actions of user"""
        user_id = kwargs.get('user_id')
        page = kwargs.get('page')
        page_size = kwargs.get('page_size')
        order_column = kwargs.get('order_column')
        order_direction = kwargs.get('order_direction')
        types = kwargs.get('types')

        if len(types) < 1 or page_size < 0:
            self.status = 401 if str(self.status)[0] < '4' else self.status
            self.message.append(_("Error request parameters: [{params}]")
                                .format(params={'types': types, 'page_size': page_size})
                                )
            return {}

        query = (
            db.session.query(Log, User.username, Dashboard, Slice)
                .join(User, Log.user_id == User.id)
                .outerjoin(Dashboard,
                           and_(
                               Log.obj_id == Dashboard.id,
                               Log.obj_type.ilike('dashboard'))
                           )
                .outerjoin(Slice,
                           and_(
                               Log.obj_id == Slice.id,
                               Log.obj_type.ilike('slice'))
                           )
                .filter(Log.user_id == user_id,
                        Log.action_type.in_(types))
        )
        count = query.count()

        # if order_column:
        #     column = self.str_to_column_in_actions.get(order_column)
        #     if order_direction == 'desc':
        #         query = query.order_by(column.desc())
        #     else:
        #         query = query.order_by(column)
        # else:
        #     query = query.order_by(Log.dttm.desc())
        query = query.order_by(Log.id.desc())

        if page_size and page_size > 0:
            query = query.limit(page_size)
        if page is not None and page >= 0:
            query = query.offset(page * page_size)

        rows = []
        for log, username, dash, slice in query.all():
            if dash:
                title, link = dash.name, dash.url
            elif slice:
                title, link = slice.name, slice.slice_url
            else:
                link = None
                g = re.compile(r"\[.*\]").search(log.action)
                title = g.group(0)[1:-1] if g else 'No this object'
            line = {'user': username,
                    'action': log.action,
                    'title': title,
                    'link': link,
                    'obj_type': log.obj_type,
                    'time': str(log.dttm)
                    }
            rows.append(line)
        return count, rows

    @catch_exception
    @expose('/actions/')
    def get_user_actions_by_url(self):
        kwargs = self.get_request_args(request.args)
        kwargs['user_id'] = g.user.id
        kwargs['types'] = request.args.get('types', self.default_types.get('actions'))

        if not isinstance(kwargs['types'], list) or len(kwargs['types']) < 1:
            message_ = _("Error request parameters: [{params}]")\
                .format(params=request.args)
            raise ParameterException(message_)

        count, data = self.get_user_actions(**kwargs)
        status_ = self.status
        message_ = self.message
        self.status = 201
        self.message = []
        if str(self.status)[0] != '2':
            return json_response(message=message_, status=status_, code=1)
        else:
            response = {}
            response['data'] = data
            response['count'] = count
            response['page'] = kwargs.get('page')
            response['page_size'] = kwargs.get('page_size')
            response['order_column'] = kwargs.get('order_column')
            response['order_direction'] = kwargs.get('order_direction')
            return json_response(data=response)

    @catch_exception
    @expose('/')
    def home(self):
        """default page"""
        if g.user.get_id() is None:
            return redirect(appbuilder.get_url_for_login)
        self.init_examples_perms()
        self.update_redirect()
        return self.render_template('superset/home.html')

    @catch_exception
    @expose('/alldata/')
    def get_all_statistics_data(self):
        self.set_global_read()
        username = g.user.username
        user_id = g.user.id
        response = {}
        #
        types = self.default_types.get('counts')
        result = self.get_object_counts(username, types)
        response['counts'] = result
        #
        types = self.default_types.get('trends')
        limit = self.default_limit.get('trends')
        today_counts = copy.deepcopy(result)
        result = self.get_object_number_trends(username, types, today_counts, limit)
        response['trends'] = result
        # #
        types = self.default_types.get('favorits')
        limit = self.default_limit.get('favorits')
        result = self.get_fav_objects(username, types, limit)
        response['favorits'] = result
        # #
        limit = self.default_limit.get('refers')
        result = self.get_refered_slices(username, limit)
        response['refers'] = result
        #
        types = self.default_types.get('edits')
        limit = self.default_limit.get('edits')
        result = self.get_edited_objects(
            username=username, types=types, page_size=limit)
        response['edits'] = result
        # #
        limit = self.default_limit.get('actions')
        types = self.default_types.get('actions')
        count, result = self.get_user_actions(
            user_id=user_id, types=types, page_size=limit)
        response['actions'] = result

        status_ = self.status
        if len(self.message) > 0:
            response['error'] = '. '.join(self.message)
        self.status = 201
        self.message = []
        return json_response(data={'index': response})

    def set_global_read(self):
        if self.guardian_auth:
            from superset.guardian import guardian_client as client
            if not client.check_global_read(g.user.username):
                self.global_read = False
        else:
            self.global_read = True
