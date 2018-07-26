import json
import logging
import os
import threading
from superset.exception import ParameterException

lock = threading.Lock()


class CheckTicketMinix(object):
    """
    """
    @staticmethod
    def is_service_ticket(ticket):
        return True if ticket.startswith('ST-') else False

    @staticmethod
    def is_proxy_grant_ticket(ticket):
        return True if ticket.startswith('PGT-') else False

    @staticmethod
    def is_proxy_grant_ticket_iou(ticket):
        return True if ticket.startswith('PGTIOU-') else False


class BaseStore(object):

    def get(self, key):
        data = self.get_data_dict()
        return data.get(key, None)

    def set(self, key, value, clear_logout=False):
        data = self.get_data_dict()
        data[key] = value
        self.set_data_dict(data)

    def remove(self, key):
        data = self.get_data_dict()
        data.pop(key, None)
        self.set_data_dict(data)

    def get_data_dict(self):
        return {}

    def set_data_dict(self, data):
        raise NotImplemented

    def check_key(self, key):
        pass

    def check_value(self, value):
        pass


class FileStore(BaseStore):
    """Use file to store a dict

    """
    path = '/tmp/pilot/store'

    def __init__(self):
        self.filename = 'file.store'
        if not os.path.exists(self.path):
            os.makedirs(self.path)

    def get_data_dict(self):
        logging.debug('[CAS] Get store file: [{}]'.format(self.filepath))
        if not os.path.exists(self.filepath):
            os.mknod(self.filepath)
            return {}
        else:
            with open(self.filepath, 'r') as f:
                data = f.read()
            try:
                return json.loads(data) if data else {}
            except json.decoder.JSONDecodeError:
                logging.exception('[CAS] {}\'s data is not a dict, will be removed: {}'
                              .format(self.filename, data))
                #os.remove(self.filepath)

    def set_data_dict(self, data):
        logging.debug('[CAS] Write store file: [{}]'.format(self.filepath))
        if not isinstance(data, dict):
            raise ParameterException('[CAS] Parameter [data] is not a dict')
        data = json.dumps(data)

        lock.acquire()
        with open(self.filepath, 'w') as f:
            f.write(data)
        lock.release()

    @property
    def filepath(self):
        return os.path.join(self.path, self.filename)


class EnvVariableStore(BaseStore):
    """Use System environment variable to store data

    Deprecated: os.environ can't be shared between process.
    """

    def __init__(self):
        self.env_key = 'ENV_VARIABLE_STORE'

    def get_data_dict(self):
        return json.loads(os.environ.get(self.env_key, '{}'))

    def set_data_dict(self, data):
        os.environ[self.env_key] = json.dumps(data)


class ProxyGrantTicketStore(FileStore, CheckTicketMinix):
    """Store proxy grant ticket in EnvVariableStore

    key: pgtiou
    value: pgt
    """
    def __init__(self):
        super(ProxyGrantTicketStore, self).__init__()
        self.filename = 'proxy_grant_ticket.store'

    def get(self, key):
        if self.is_key(key):
            return super(ProxyGrantTicketStore, self).get(key)
        else:
            return self.get_by_value(key)

    def get_by_value(self, value):
        data = self.get_data_dict()
        for k, v in data.items():
            if v == value:
                return k
        return None

    def remove(self, key):
        if not self.is_key(key):
            key = self.get_by_value(key)
        return super(ProxyGrantTicketStore, self).remove(key)

    def is_key(self, key):
        return self.is_proxy_grant_ticket_iou(key)

    def check_key(self, key):
        if not self.is_proxy_grant_ticket_iou(key):
            raise Exception

    def check_value(self, value):
        if not self.is_proxy_grant_ticket(value):
            raise Exception


class ServiceTicketStore(FileStore, CheckTicketMinix):
    """Store service ticket in EnvVariableStore

    key: service ticket
    value: username
    """
    LOGOUT_MARK = '_LOGOUT_'
    VERIFIED_MARK = '_VERIFIED_'

    def __init__(self):
        super(ServiceTicketStore, self).__init__()
        self.filename = 'service_ticket.store'

    def set(self, key, value, clear_logout=False):
        data = self.get_data_dict()
        # Remove old service ticket
        if clear_logout:
            for k in list(data):
                v = data[k]
                if v.startswith(self.VERIFIED_MARK) or v.endswith(self.LOGOUT_MARK):
                    data.pop(k)
        data[key] = value
        self.set_data_dict(data)

    def check_key(self, key):
        if not self.is_service_ticket(key):
            raise Exception()

    def logout(self, key):
        self.check_key(key)
        data = self.get_data_dict()
        if key in data:
            data[key] = '{}{}'.format(data[key], self.LOGOUT_MARK)
        self.set_data_dict(data)

    def is_logout(self, key):
        value = self.get(key)
        return True if value is None or value.endswith(self.LOGOUT_MARK) else False

    def verify(self, key):
        self.check_key(key)
        data = self.get_data_dict()
        if key in data:
            data[key] = '{}{}'.format(self.VERIFIED_MARK, data[key])
        self.set_data_dict(data)

    def is_verified(self, key):
        value = self.get(key)
        return True if value is None or value.startswith(self.VERIFIED_MARK) else False


class CASSessionStore(CheckTicketMinix):
    """
    """
    def __init__(self):
        self.st_store = ServiceTicketStore()
        self.pgt_store = ProxyGrantTicketStore()

    def get(self, key):
        logging.debug('[CAS] CASClientSession gets [{}...]'.format(key[:20]))
        if key is not None:
            if self.is_service_ticket(key):
                return self.st_store.get(key)
            elif self.is_proxy_grant_ticket(key) or self.is_proxy_grant_ticket_iou(key):
                return self.pgt_store.get(key)
        raise Exception('[CAS] Error format of key [{}...]'.format(key[:20]))

    def record(self, key, value, clear_logout=False):
        logging.debug('[CAS] CASClientSession adds [{}...]: [{}...]'
                      .format(key[:20], value[:20]))
        if key is None or value is None:
            raise Exception('[CAS] key [{}...] or value [{}...] is None'
                            .format(key[:20], value[:20]))
        if self.is_service_ticket(key):
            self.st_store.set(key, value, clear_logout)
        elif self.is_proxy_grant_ticket_iou(key):
            self.pgt_store.set(key, value, clear_logout)
        else:
            raise Exception('[CAS] Error format of key [{}...]'.format(key[:20]))

    def destroy(self, key):
        logging.debug('[CAS] CASClientSession removes [{}...]'.format(key[:20]))
        if key is None:
            raise Exception('[CAS] key is None')
        if self.is_service_ticket(key):
            self.st_store.remove(key)
        elif self.is_proxy_grant_ticket_iou(key) or self.is_proxy_grant_ticket(key):
            self.pgt_store.remove(key)
        else:
            raise Exception('[CAS] Error format of key [{}...]'.format(key[:20]))

    def logout_st(self, key):
        logging.debug('[CAS] CASClientSession logout [{}...]'.format(key[:20]))
        if key is not None:
            self.st_store.logout(key)
        else:
            logging.error('[CAS] CASClientSession logout key is None')

    def verify_st(self, key):
        logging.debug('[CAS] CASClientSession verifies [{}...]'.format(key[:20]))
        if key is not None:
            self.st_store.verify(key)
        else:
            logging.error('[CAS] CASClientSession verify key is None')

    def is_logout_st(self, key):
        return self.st_store.is_logout(key)

    def is_verified_st(self, key):
        return self.st_store.is_verified(key)


cas_session = CASSessionStore()
