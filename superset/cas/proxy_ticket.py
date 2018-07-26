import flask
import logging
from xmltodict import parse
from flask import current_app

from . import keys
from .cas_urls import create_cas_proxy_url
from .cas_session import cas_session

try:
    from urllib import urlopen
except ImportError:
    from urllib.request import urlopen


def get_proxy_ticket(target_service):
    """
    Get 'proxy ticket for' 'target_service'.
    """
    pgtiou = flask.session[keys.CAS_PGTIOU]
    if not pgtiou:
        raise Exception('[CAS] No CAS_PGTIOU in session')
    pgt = cas_session.get(pgtiou)
    if not pgt:
        raise Exception('[CAS] Not found PGT by PGTIOU: {}...'.format(pgtiou[:20]))

    cas_proxy_url = create_cas_proxy_url(
        current_app.config[keys.CAS_SERVER],
        current_app.config[keys.CAS_PROXY_ROUTE],
        target_service,
        pgt)
    logging.info('[CAS] Try to get proxy ticket for service: {} by PGT: {}...'
                 .format(target_service, pgt[0:20]))

    try:
        xmldump = urlopen(cas_proxy_url).read().strip().decode('utf8', 'ignore')
        xml_from = parse(xmldump)
        xml_from = xml_from['cas:serviceResponse']
        if 'cas:proxySuccess' in xml_from:
            pt = xml_from['cas:proxySuccess']['cas:proxyTicket']
            logging.info('Success to get proxy ticket: {}...'.format(pt[0:20]))
        elif 'cas:proxyFailure' in xml_from:
            raise Exception(
                '[CAS] Failed to get proxy ticket: ' + str(dict(xml_from['cas:proxyFailure'])))
        else:
            raise Exception('[CAS] Error response when getting proxy ticket: ' + xml_from)
    except ValueError as e:
        raise Exception("[CAS] CAS returned unexpected result: " + str(e))

    return pt
