#!/usr/bin/env python
# -*- coding:utf-8 -*-
# Author: jiajie.zhang@transwarp.io

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

from jpype import *
from .guardian_base import PrincipalType, GuardianBase, catch_guardian_exception


class GuardianAdmin(GuardianBase):

    def __init__(self):
        super(GuardianAdmin, self).__init__()
        admin_factory = JClass('io.transwarp.guardian.client.GuardianAdminFactory')
        self.client = admin_factory.getInstance()
        self.component = self.client.getComponent()

    @catch_guardian_exception
    def add_permission(self, finite_obj, action):
        """
        :param finite_obj: a list of object's finite_obj structure:
                           [object_type, object_id], such as ['database', 1]
        :param action:
        :return:
        """
        if isinstance(action, list):
            for a in action:
                new_perm = self._permission(finite_obj, a)
                self.client.addPermission(new_perm)
        else:
            new_perm = self._permission(finite_obj, action)
            self.client.addPermission(new_perm)

    @catch_guardian_exception
    def del_permission(self, finite_obj, action):
        if isinstance(action, list):
            for a in action:
                perm = self._permission(finite_obj, a)
                self.client.delPermission(perm)
        else:
            perm = self._permission(finite_obj, action)
            self.client.delPermission(perm)

    @catch_guardian_exception
    def rename_perm_obj(self, old_finite_obj, new_finite_obj):
        old = self._perm_obj(old_finite_obj)
        new = self._perm_obj(new_finite_obj)
        self.client.renamePermObj(old, new)

    @catch_guardian_exception
    def del_perm_obj(self, finite_obj):
        perm_obj = self._perm_obj(finite_obj)
        self.client.delPermObj(perm_obj)

    @catch_guardian_exception
    def grant(self, name, finite_obj, action, principal_type='USER'):
        if isinstance(action, list):
            for a in action:
                entity_perm = \
                    self._entity_permission(name, principal_type, finite_obj, a)
                self.client.grant(entity_perm)
        else:
            entity_perm = \
                self._entity_permission(name, principal_type, finite_obj, action)
            self.client.grant(entity_perm)

    def grant_read_perm(self, name, finite_obj, principal_type='USER'):
        self.grant(name, finite_obj, self.PERM_READ, principal_type)

    def grant_owner_perm(self, name, finite_obj, principal_type='USER'):
        self.grant(name, finite_obj, self.OWNER_PERMS, principal_type)

    def grant_admin_role(self, role_name):
        self.grant(role_name, self.GLOBAL_OBJECT, self.GLOBAL_PERM_ADMIN,
                   principal_type=PrincipalType.ROLE.value)

    def grant_developer_role(self, role_name):
        self.grant(role_name, self.GLOBAL_OBJECT, self.GLOBAL_PERM_EDIT,
                   principal_type=PrincipalType.ROLE.value)

    def grant_viewer_role(self, role_name):
        self.grant(role_name, self.GLOBAL_OBJECT, self.GLOBAL_PERM_READ,
                   principal_type=PrincipalType.ROLE.value)

    @catch_guardian_exception
    def revoke(self, name, finite_obj, action, principal_type='USER'):
        if isinstance(action, list):
            for a in action:
                entity_perm = \
                    self._entity_permission(name, principal_type, finite_obj, a)
                self.client.revoke(entity_perm)
        else:
            entity_perm = \
                self._entity_permission(name, principal_type, finite_obj, action)
            self.client.revoke(entity_perm)

    def revoke_admin_role(self, role_name):
        self.revoke(role_name, self.GLOBAL_OBJECT, self.GLOBAL_PERM_ADMIN,
                    principal_type=PrincipalType.ROLE.value)

    def revoke_developer_role(self, role_name):
        self.revoke(role_name, self.GLOBAL_OBJECT, self.GLOBAL_PERM_EDIT,
                    principal_type=PrincipalType.ROLE.value)

    def revoke_viewer_role(self, role_name):
        self.revoke(role_name, self.GLOBAL_OBJECT, self.GLOBAL_PERM_READ,
                    principal_type=PrincipalType.ROLE.value)

    @catch_guardian_exception
    def clean_perm_objs(self, finite_obj=None):
        """
        :param finite_obj: finite_obj=None, will delete all perm objects
        except [GLOBAL]; finite_obj=['SLICE', ], will delete all perm objects of
        type 'SLICE'; finite_obj=['SLICE', 'name'], will delete ['SLICE', 'name']
        :return:
        """
        client_factory = JClass('io.transwarp.guardian.client.GuardianClientFactory')
        client = client_factory.getInstance()
        if not finite_obj:
            perms = client.listPermissions(self.component)
        else:
            datasource = self._datasource(finite_obj)
            perms = client.listPermissions(self.component, datasource)
        for perm in perms:
            if perm.getDataSource() != self.global_datasource:
                perm_obj = self._perm_obj(perm.getDataSource())
                self.client.delPermObj(perm_obj)

    @catch_guardian_exception
    def create_token(self, username, password, token_name):
        self.client.login(username, password)
        token = self._access_token(username, token_name)
        new_token = self.client.createAccessToken(token)
        self.client.logout()
        return new_token.getContent()

    @catch_guardian_exception
    def add_role(self, role_name, usernames=()):
        role = self._role(role_name)
        if usernames:
            user_list = java.util.ArrayList()
            for u in usernames:
                user_list.add(u)
            role.setUsers(user_list)
        self.client.addRole(role)

    @catch_guardian_exception
    def delete_role(self, role_name):
        role = self._role(role_name)
        self.client.delRole(role)

    def add_global_perms(self):
        self.add_permission(self.GLOBAL_OBJECT,
                            [self.GLOBAL_PERM_READ, self.GLOBAL_PERM_EDIT])


guardian_admin = GuardianAdmin()
