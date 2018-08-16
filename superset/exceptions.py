# -*- coding: utf-8 -*-
# pylint: disable=C,R,W
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals


class SupersetException(Exception):
    status = 500


class SupersetTimeoutException(SupersetException):
    pass


class SupersetSecurityException(SupersetException):
    pass


class MetricPermException(SupersetException):
    pass


class NoDataException(SupersetException):
    status = 400


class NullValueException(SupersetException):
    status = 400


class SupersetTemplateException(SupersetException):
    pass


class SupersetException2(Exception):
    exception_code = 1

    def __init__(self, message=None, code=None):
        self.message = message
        self.code = code if code else self.exception_code

    def __repr__(self):
        return 'Code: [{}] Message: [{}]'.format(self.code, self.message)


class LoginException(SupersetException2):
    exception_code = 2


class ErrorRequestException(SupersetException2):
    exception_code = 3


class ParameterException(SupersetException2):
    exception_code = 4


class PropertyException(SupersetException2):
    exception_code = 5


class DatabaseException(SupersetException2):
    exception_code = 6


class HDFSException(SupersetException2):
    exception_code = 7


class PermissionException(SupersetException2):
    exception_code = 8


class GuardianException(SupersetException2):
    exception_code = 9


class TemplateException(SupersetException2):
    exception_code = 10

