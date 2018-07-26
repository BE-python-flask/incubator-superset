from flask import g
from flask_appbuilder import expose
from superset.exception import SupersetException
from superset import sm
from superset.message import *
from .base import BaseSupersetView, catch_exception, json_response


class UserView(BaseSupersetView):
    route_base = '/user'
    default_role = 'Admin'
    list_columns = ['id', 'username', 'email', 'last_login']
    show_columns = ['username', 'email', 'last_login', 'login_count', 'created_on']
    str_columns = ['last_login', 'created_on', 'changed_on']

    @catch_exception
    @expose('/list/', methods=['GET'])
    def list(self):
        users = sm.get_all_users()
        data = []
        for user in users:
            line = {}
            for col in self.list_columns:
                line[col] = str(getattr(user, col))
            line['created_by'] = user.created_by.username \
                if user.created_by else None
            data.append(line)
        return json_response(data={'count': len(users), 'data': data})

    @catch_exception
    @expose('/add/', methods=['POST'])
    def add(self):
        data = self.get_request_data()
        username = data.get('username')
        password = data.get('password')
        email = data.get('email')
        user = self.appbuilder.sm.add_user(
            username, username, username, email,
            self.appbuilder.sm.find_role(self.default_role),
            password=password)
        if not user:
            return json_response(message=ADD_FAILED, status=500)
        return json_response(message=ADD_SUCCESS)

    @catch_exception
    @expose('/show/<pk>/', methods=['GET'])
    def show(self, pk):
        user = sm.get_user_by_id(pk)
        data = {}
        for col in self.show_columns:
            data[col] = str(getattr(user, col))
        data['created_by'] = user.created_by.username if user.created_by else None
        return json_response(data=data)

    @catch_exception
    @expose('/edit/<pk>/', methods=['POST'])
    def edit(self, pk):
        data = self.get_request_data()
        username = data.get('username')
        user = sm.get_user_by_id(pk)
        self.check_permission(user, 'edit')
        user.first_name = username
        user.last_name = username
        user.username = username
        user.email = data.get('email')
        rs = sm.update_user(user)
        if rs is False:
            return json_response(message=UPDATE_FAILED, status=500)
        rs = sm.reset_password(pk, data.get('password'))
        if rs is False:
            return json_response(message='Update password failed', status=500)
        return json_response(message=UPDATE_SUCCESS)

    @catch_exception
    @expose('/delete/<pk>/', methods=['GET'])
    def delete(self, pk):
        user = sm.get_user_by_id(pk)
        self.check_permission(user, 'delete')
        rs = sm.del_register_user(user)
        if not rs:
            return json_response(message=DELETE_FAILED, status=500)
        return json_response(message=DELETE_SUCCESS, status=500)

    @classmethod
    def check_permission(cls, user, action):
        present_id = g.user.get_user_id()
        if action == 'edit':
            if user.id == present_id and user.created_by_fk == present_id:
                return True
            else:
                raise SupersetException('No permission to edit this user.')
        elif action == 'delete':
            if user.created_by_fk == present_id:
                return True
            else:
                raise SupersetException('No permission to delete this user.')
        else:
            raise SupersetException('Error action [{}]'.format(action))


class PresentUserView(BaseSupersetView):
    route_base = '/present_user'

    @catch_exception
    @expose('/')
    def home(self):
        self.update_redirect()
        return self.render_template('superset/user.html')

    @catch_exception
    @expose('/show/', methods=['GET'])
    def show(self):
        user = g.user
        if not user:
            raise SupersetException(NO_USER)

        data = {'username': user.username,
                'login_count': user.login_count,
                'last_login': str(user.last_login)}
        return json_response(data=data)
