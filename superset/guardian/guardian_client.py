#!/usr/bin/env python
# -*- coding:utf-8 -*-
# Author: jiajie.zhang@transwarp.io

import logging
from jpype import *
from .guardian_base import GuardianBase, catch_guardian_exception


class GuardianClient(GuardianBase):

    def __init__(self):
        super(GuardianClient, self).__init__()
        client_factory = JClass('io.transwarp.guardian.client.GuardianClientFactory')
        self.client = client_factory.getInstance()
        self.component = self.client.getComponent()

    @catch_guardian_exception
    def register(self):
        service_type = self.service_type
        component = self.component
        description = "Service {}".format(self.component)
        config = JObject({}, JClass('java.util.Map'))
        self.client.register(service_type, component, description, config)

    @catch_guardian_exception
    def authenticate(self, username, password):
        self.client.authenticate(username, password)

    @catch_guardian_exception
    def get_user(self, username):
        user = self.client.getUser(username)
        username = user.getUserName()
        print(username)

    @catch_guardian_exception
    def get_users(self, prefix=None):
        if not prefix:
            users = self.client.getUsers()
        else:
            users = self.client.getUsers(prefix)
        return sorted([user.getUserName() for user in users])

    def _check_access(self, username, permission):
        return self.client.checkAccess(username, permission)

    def _check_any_access(self, username, permissions):
        return self.client.checkAnyAccess(username, permissions)

    @catch_guardian_exception
    def check_access(self, username, finite_obj, action):
        """
        :param username:
        :param finite_obj: a list of object's finite_obj structure:
                           [object_type, object_name], such as ['database', 'xxx]
        :param actions:
        :return:
        """
        if isinstance(action, list):
            perm = self._permissions(finite_obj, action)
        else:
            perm = self._permission(finite_obj, action)
        can = self._check_access(username, perm)
        return True if can else False

    @catch_guardian_exception
    def check_any_access(self, username, finite_obj, actions):
        perms = self._permissions(finite_obj, actions)
        can = self._check_any_access(username, perms)
        return True if can else False

    @catch_guardian_exception
    def check_global_admin(self, username):
        return self._check_access(username, self._global_perm_admin())

    @catch_guardian_exception
    def check_global_edit(self, username):
        perms = self._permissions(self.GLOBAL_OBJECT, self.GLOBAL_PERMS_EDIT)
        return self.client.checkAnyAccess(username, perms)

    @catch_guardian_exception
    def check_global_read(self, username):
        perms = self._permissions(self.GLOBAL_OBJECT, self.GLOBAL_PERMS_READ)
        return self.client.checkAnyAccess(username, perms)

    def check_read_access(self, username, finite_obj):
        return self.check_any_access(username, finite_obj, self.PERMS_READ)

    def check_edit_access(self, username, finite_obj):
        return self.check_any_access(username, finite_obj, self.PERMS_EDIT)

    def check_delete_access(self, username, finite_obj):
        return self.check_edit_access(username, finite_obj)

    def check_admin_access(self, username, finite_obj):
        return self.check_access(username, finite_obj, self.PERM_ADMIN)

    def check_grant_access(self, username, finite_obj):
        return self.check_admin_access(username, finite_obj)

    def check_revoke_access(self, username, finite_obj):
        return self.check_admin_access(username, finite_obj)

    def check_release_access(self, username, finite_obj):
        return self.check_admin_access(username, finite_obj)

    @catch_guardian_exception
    def user_permissions(self, username, component=None, finite_obj=None):
        if not component:
            perms = self.client.userPermissions(username)
        elif not finite_obj:
            perms = self.client.userPermissions(username, component)
        else:
            datasource = self._datasource(finite_obj)
            perms = self.client.userPermissions(username, component, datasource)
        for perm in perms:
            print('{} {} {}'.format(perm.getComponent(), perm.getDataSource(),
                                    perm.getAction()))

    @catch_guardian_exception
    def list_permissions(self, component=None, finite_obj=None):
        component = component if component else self.component
        if not finite_obj:
            perms = self.client.listPermissions(component)
        else:
            datasource = self._datasource(finite_obj)
            perms = self.client.listPermissions(component, datasource)
        for perm in perms:
            print('{} {} {}'.format(perm.getComponent(), perm.getDataSource(),
                                    perm.getAction()))

    @catch_guardian_exception
    def search_object_permissions(self, finite_obj, component=None):
        """
        Search users' permission on a specific object
        :param finite_obj: [object_type, object_id], such as ['database', 1]
        :param component: default component in guardian-site.xml
        :return: {user1: [action1,], user2: [action2,action2], }
        """
        component = component if component else self.component
        datasource = self._datasource(finite_obj)
        entity_perms = self.client.searchPermissions(component, datasource)
        data = {}
        for entity_perm in entity_perms:
            ptype = entity_perm.getPrincipalType()
            if ptype and str(ptype).upper() == 'USER':
                name = entity_perm.getName()
                perm = entity_perm.getPermissionVo()
                if name in data.keys():
                    actions = data.get(name)
                    actions.append(perm.getAction())
                    data[name] = actions
                else:
                    data[name] = [perm.getAction(), ]
        return data

    @catch_guardian_exception
    def search_model_perms(self, username, model, component=None):
        """
        Search user's permissions of all objects of one model
        :param username:
        :param model: such as 'database', 'dataset', 'slice', 'dashboard' ...
        :param component:
        :return: set of ids which has any permissions
        """
        component = component if component else self.component
        datasource = self._datasource([model.upper(), ])
        entity_perms = self.client.searchPermissions(
            username, self.PrincipalType.USER, component, datasource, True
        )
        names = set()
        for entity_perm in entity_perms:
            perm = entity_perm.getPermissionVo()
            datasource = perm.getDataSource()
            name = datasource.get(datasource.size() - 1)
            if name:
                # if id in data.keys():
                #     actions = data.get(id)
                #     actions.append(perm.getAction())
                #     data[id] = actions
                # else:
                #     data[id] = [perm.getAction(), ]
                names.add(name)
        return sorted(names)

    @catch_guardian_exception
    def download_keytab(self, username, password, file_path):
        self.client.login(username, password)
        keytab = self.client.getKeytab(username)
        self.client.logout()
        FileUtils = JClass('org.apache.commons.io.FileUtils')
        File = JClass('java.io.File')
        file = File(file_path)
        FileUtils.writeByteArrayToFile(file, keytab)

    @catch_guardian_exception
    def get_token(self, username, password, token_name):
        self.client.login(username, password)
        tokens = self.client.getAccessTokenByOwner(username)
        token_id = None
        for token in tokens:
            if token.getName() == token_name:
                token_id = token.getId().value
                break
        if token_id:
            token = self.client.getAccessTokenById(token_id)
            return token.getContent()
        else:
            return None

    @catch_guardian_exception
    def get_role(self, role_name):
        try:
            return self.client.getRole(role_name)
        except Exception as e:
            logging.info('Failed to get role [{}].'.format(role_name))
            logging.error((str(e)))
        return None


guardian_client = GuardianClient()
