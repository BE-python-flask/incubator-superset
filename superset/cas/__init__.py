"""
flask_cas.__init__
"""

import flask
from flask import current_app

# Find the stack on which we want to store the database connection.
# Starting with Flask 0.9, the _app_ctx_stack is the correct one,
# before that we need to use the _request_ctx_stack.
try:
    from flask import _app_ctx_stack as stack
except ImportError:
    from flask import _request_ctx_stack as stack

from functools import wraps
from . import routing, keys


class CAS(object):
    """
    Required Configs:

    |Key             |
    |----------------|
    |CAS_SERVER      |
    |CAS_AFTER_LOGIN |

    Optional Configs:

    |Key                            | Default                        |
    |-------------------------------|--------------------------------|
    |CAS_SERVICE_TICKET_SESSION_KEY | CAS_SERVICE_TICKET             |
    |CAS_USERNAME_SESSION_KEY       | CAS_USERNAME                   |
    |CAS_ATTRIBUTES_SESSION_KEY     | CAS_ATTRIBUTES                 |
    |CAS_LOGIN_ROUTE                | 'url_prefix/login'             |
    |CAS_LOGOUT_ROUTE               | 'url_prefix/logout'            |
    |CAS_SERVICE_VALIDATE_ROUTE     | 'url_prefix/p3/serviceValidate'|
    |CAS_PROXY_ROUTE                | 'url_prefix/proxy'             |
    |CAS_PROXY_VALIDATE_ROUTE       | 'url_prefix/p3/proxyValidate'  |
    |CAS_AFTER_LOGOUT               | None                           |
    """

    def __init__(self, app=None, url_prefix=None):
        self._app = app
        if app is not None:
            self.init_app(app, url_prefix)

    def init_app(self, app, url_prefix='/cas'):
        # Configuration defaults
        app.config.setdefault(keys.CAS_LOGIN_ROUTE, '{}/login'.format(url_prefix))
        app.config.setdefault(keys.CAS_LOGOUT_ROUTE, '{}/logout'.format(url_prefix))
        app.config.setdefault(keys.CAS_PROXY_ROUTE, '{}/proxy'.format(url_prefix))
        app.config.setdefault(keys.CAS_SERVICE_VALIDATE_ROUTE, '{}/p3/serviceValidate'.format(url_prefix))
        app.config.setdefault(keys.CAS_PROXY_VALIDATE_ROUTE, '{}/p3/proxyValidate'.format(url_prefix))
        app.config.setdefault(keys.CAS_PROXY_CALLBACK_ROUTE, '{}/proxyCallback'.format(url_prefix))
        # Requires CAS 2.0
        app.config.setdefault(keys.CAS_AFTER_LOGIN, '/')
        app.config.setdefault(keys.CAS_AFTER_LOGOUT, None)
        # Register Blueprint
        app.register_blueprint(routing.blueprint, url_prefix=url_prefix)

        # Use the newstyle teardown_appcontext if it's available,
        # otherwise fall back to the request context
        if hasattr(app, 'teardown_appcontext'):
            app.teardown_appcontext(self.teardown)
        else:
            app.teardown_request(self.teardown)

    def teardown(self, exception):
        ctx = stack.top

    @property
    def app(self):
        return self._app or current_app

    @property
    def username(self):
        return flask.session.get(keys.CAS_USERNAME, None)

    @property
    def attributes(self):
        return flask.session.get(keys.CAS_ATTRIBUTES, None)

    @property
    def service_ticket(self):
        return flask.session.get(keys.CAS_SERVICE_TICKET, None)

    @property
    def pgtiou(self):
        return flask.session.get(keys.CAS_PGTIOU, None)


def login():
    return flask.redirect(flask.url_for('cas.login', _external=True))


def logout():
    return flask.redirect(flask.url_for('cas.logout', _external=True))


def login_required(function):
    @wraps(function)
    def wrap(*args, **kwargs):
        if keys.CAS_USERNAME not in flask.session:
            flask.session[keys.CAS_AFTER_LOGIN_SESSION_URL] = flask.request.path
            return login()
        else:
            return function(*args, **kwargs)
    return wrap
