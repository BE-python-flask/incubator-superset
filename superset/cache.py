import logging
import threading
from flask import request

from superset import simple_cache, tables_cache
from superset.config import (
    GUARDIAN_AUTH, CAS_AUTH, GUARDIAN_ACCESS_TOKEN_NAME as TOKEN_NAME
)
from superset.cas.access_token import get_token as get_token_by_cas


mutex = threading.Lock()


def view_cache_key(*unused_args, **unused_kwargs):
    args_hash = hash(frozenset(request.args.items()))
    return 'view/{}/{}'.format(request.path, args_hash)


def memoized_func(timeout=5 * 60, key=view_cache_key):
    """Use this decorator to cache functions that have predefined first arg.

    memoized_func uses simple_cache and stored the data in memory.
    Key is a callable function that takes function arguments and
    returns the caching key.
    """
    def wrap(f):
        if tables_cache:
            def wrapped_f(cls, *args, **kwargs):
                cache_key = key(*args, **kwargs)
                o = tables_cache.get(cache_key)
                if not kwargs['force'] and o is not None:
                    return o
                o = f(cls, *args, **kwargs)
                tables_cache.set(cache_key, o, timeout=timeout)
                return o
        else:
            # noop
            def wrapped_f(cls, *args, **kwargs):
                return f(cls, *args, **kwargs)
        return wrapped_f
    return wrap


class BaseCache(object):

    @classmethod
    def do_cache(cls, key, value, timeout=86400):
        try:
            if mutex.acquire(1):
                simple_cache.set(key, value, timeout=timeout)
                logging.info('Cached [{}]'.format(key))
                mutex.release()
        except Exception as e:
            logging.exception(e)
            raise e

    @classmethod
    def clear_cache(cls):
        simple_cache.clear()


class TokenCache(BaseCache):

    KEY = 'Token_{username}'
    TIMEOUT = 7 * 86400

    @classmethod
    def key(cls, username):
        return cls.KEY.format(username=username)

    @classmethod
    def cache(cls, username, token):
        cls.do_cache(cls.key(username), token, timeout=cls.TIMEOUT)

    @classmethod
    def get(cls, username, password):
        key = cls.key(username)
        token = simple_cache.get(key)
        if token:
            logging.info('Got Token [{}] from cache'.format(key))
        else:
            logging.info('Token [{}] is not cached. Try to cache...'.format(key))
            token = cls.get_token(username, password)
            cls.do_cache(key, token)
        return token

    @classmethod
    def get_token(cls, username, password):
        if GUARDIAN_AUTH and not CAS_AUTH:  # When CAS auth, password is incorrect.
            from superset.guardian import guardian_client, guardian_admin
            token = guardian_client.get_token(username, password, TOKEN_NAME)
            if not token:
                token = guardian_admin.create_token(username, password, TOKEN_NAME)
            logging.info('Got token by Guardian Client API: [{}...]'.format(token[:15]))
            return token
        elif CAS_AUTH:
            token = get_token_by_cas(username, TOKEN_NAME)
            logging.info('Got token by CAS: [{}...]'.format(token[:15]))
            return token
        else:
            logging.info("Not enable Guardian and CAS, can't get Access Token")
            return None

    @classmethod
    def delete(cls, cache, username):
        cache.delete(cls.key(username))


class FileRobotCache(BaseCache):
    """Deprecated. Not cache Filerobot client anymore.
    """
    KEY = 'Filerobot_{username}_{httpfs}'
    TIMEOUT = 86400

    @classmethod
    def key(cls, username, httpfs):
        return cls.KEY.format(username=username, httpfs=httpfs)

    @classmethod
    def cache(cls, username, httpfs, filerobot_client):
        key = cls.key(username, httpfs)
        cls.do_cache(key, filerobot_client, timeout=cls.TIMEOUT)

    @classmethod
    def get(cls, username, httpfs):
        key = cls.key(username, httpfs)
        client = simple_cache.get(key)
        if client:
            logging.info('Got Filerobot client [{}] from cache'.format(key))
        else:
            logging.error('Filerobot client [{}] is not cached'.format(key))
        return client

    @classmethod
    def delete(cls, username, httpfs):
        simple_cache.delete(cls.key(username, httpfs))
