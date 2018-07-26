#!/usr/bin/env python
# -*- coding:utf-8 -*-
# Author: jiajie.zhang@transwarp.io

import functools
import logging
import re
from enum import Enum
from jpype import *
from superset import conf
from superset.exception import GuardianException, SupersetException


RELOGIN_CODES = ['401', '61010']


def need_login_guardian(error_msg):
    def extract_error_msg(error_msg):
        matchs = re.findall(r"ErrorCode: (.+?), ErrorMessage: (.+)", error_msg)
        if matchs:
            return {'code': str(matchs[0][0]), 'message': matchs[0][1]}
        else:
            logging.error('Failed to match error message: [{}]'.format(error_msg))
            return {'code': '-1', 'message': None}

    error = extract_error_msg(error_msg)
    logging.info('Guardian exception: {}'.format(error))
    if error['code'] in RELOGIN_CODES:
        return True
    else:
        return False


def catch_guardian_exception(f):
    """
    A decorator to label an endpoint as an API. Catches uncaught exceptions and
    return the response in the JSON format
    """
    def wraps(self, *args, **kwargs):
        try:
            return f(self, *args, **kwargs)
        except JavaException as e:
            if need_login_guardian(str(e)):

                try:
                    self.login()
                    logging.info('Login guardian again...')
                    return f(self, *args, **kwargs)
                except JavaException as e:
                    logging.exception(e.stacktrace())
                    raise GuardianException(str(e))
                except Exception as e:
                    raise SupersetException(str(e))

            else:
                logging.exception(e.stacktrace())
                raise GuardianException(str(e))
        except Exception as e:
            raise SupersetException(str(e))
    return functools.update_wrapper(wraps, f)


class PrincipalType(Enum):
    USER = 'USER'
    ROLE = 'ROLE'
    GROUP = 'GROUP'


class GuardianBase(object):

    GLOBAL_OBJECT = ['GLOBAL', ]
    GLOBAL_PERM_ADMIN = 'ADMIN'
    GLOBAL_PERM_EDIT = 'EDIT'
    GLOBAL_PERM_READ = 'READ'
    GLOBAL_PERMS_EDIT = ['ADMIN', 'EDIT']
    GLOBAL_PERMS_READ = ['ADMIN', 'EDIT', 'READ']

    PERM_READ = 'READ'
    PERM_EDIT = 'EDIT'
    PERM_ADMIN = 'ADMIN'
    ALL_PERMS = [PERM_READ, PERM_EDIT, PERM_ADMIN]
    OWNER_PERMS = ALL_PERMS
    PERMS_READ = ALL_PERMS
    PERMS_EDIT = [PERM_EDIT, PERM_ADMIN]
    PERMS_ADMIN = [PERM_ADMIN, ]

    def __init__(self):
        self.client = None
        self.component = None
        self.global_datasource = self._datasource(self.GLOBAL_OBJECT)
        self.service_type = conf.get('GUARDIAN_SERVICE_TYPE')
        self.models = JPackage('io.transwarp.guardian.common.model')
        self.PermissionVo = self.models.PermissionVo
        self.EntityPermissionVo = self.models.EntityPermissionVo
        self.PrincipalType = self.models.PrincipalType
        self.UserVo = self.models.UserVo
        self.PermObjVo = self.models.PermObjVo
        self.AccessTokenVo = self.models.AccessTokenVo
        self.RoleVo = self.models.RoleVo

    @catch_guardian_exception
    def login(self, username=None, password=None):
        if not username and not password:
            self.client.login()
        else:
            self.client.login(username, password)

    def _datasource(self, finite_obj):
        """Datasource is like ['DATABASE', 'name']"""
        alist = java.util.ArrayList()
        alist.add(str(finite_obj[0]).upper())
        if len(finite_obj) > 1:
            for s in finite_obj[1:]:
                alist.add(str(s))
        return alist

    def _permission(self, finite_obj, action):
        datasource = self._datasource(finite_obj)
        if isinstance(action, list):
            action = action[0]
        return self.PermissionVo(self.component, datasource, action)

    def _permissions(self, finite_obj, actions):
        datasource = self._datasource(finite_obj)
        perms = java.util.ArrayList()
        if not isinstance(actions, list):
            perm = self.PermissionVo(self.component, datasource, actions)
            perms.add(perm)
        else:
            for action in actions:
                perm = self.PermissionVo(self.component, datasource, action)
                perms.add(perm)
        return perms

    def _entity_permission(self, name, principal_type, finite_obj, action):
        if principal_type.upper() == PrincipalType.GROUP.value:
            principal_type = self.PrincipalType.GROUP
        elif principal_type.upper() == PrincipalType.ROLE.value:
            principal_type = self.PrincipalType.ROLE
        else:
            principal_type = self.PrincipalType.USER
        permission = self._permission(finite_obj, action)
        return self.EntityPermissionVo(name, principal_type, permission)

    def _perm_obj(self, finite_obj):
        datasource = self._datasource(finite_obj)
        return self.PermObjVo(self.component, datasource)

    def _access_token(self, owner, token_name):
        token = self.AccessTokenVo(token_name)
        token.setOwner(owner)
        return token

    def _role(self, role_name):
        return self.RoleVo(role_name)

    def _global_perm_admin(self):
        return self._permission(self.GLOBAL_OBJECT, self.GLOBAL_PERM_ADMIN)

    def _global_perm_edit(self):
        return self._permission(self.GLOBAL_OBJECT, self.GLOBAL_PERM_EDIT)

    def _global_perm_read(self):
        return self._permission(self.GLOBAL_OBJECT, self.GLOBAL_PERM_READ)

    def _perm_read(self, finite_obj):
        return self._permission(finite_obj, self.PERM_READ)

    def _perm_edit(self, finite_obj):
        return self._permission(finite_obj, self.PERM_EDIT)

    def _perm_admin(self, finite_obj):
        return self._permission(finite_obj, self.PERM_ADMIN)
