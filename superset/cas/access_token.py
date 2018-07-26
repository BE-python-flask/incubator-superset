import json
import logging
import requests
from flask import current_app

from .proxy_ticket import get_proxy_ticket


def guardian_server():
    server = current_app.config.get('GUARDIAN_SERVER')
    if 'http://' not in server and 'https://' not in server:
        if '8080' in server:
            server = 'http://{}'.format(server)
        else:
            server = 'https://{}'.format(server)
    return server


def get_token(username, token_name):
    target_service = guardian_server()
    pt = get_proxy_ticket(target_service)
    url = '{}/api/v1/accessToken?owner={}&ticket={}'.format(target_service, username, pt)
    logging.info(url)

    resp = requests.get(url, verify=False)
    try:
        token_objs = json.loads(resp.text)
    except Exception as e:
        raise Exception('[CAS] Error response when getting tokens: ' + str(resp.text))
    for token_obj in token_objs:
        if token_obj['name'] == token_name:
            return get_token_by_id(target_service, token_obj['id'])

    return create_token(target_service, token_name)


def get_token_by_id(service, id):
    pt = get_proxy_ticket(service)
    url = '{}/api/v1/accessToken/{}?ticket={}'.format(service, id, pt)
    logging.info(url)

    resp = requests.get(url, verify=False)
    try:
        token_obj = json.loads(resp.text)
    except Exception as e:
        raise Exception('[CAS] Error response when getting token by id: ' + str(resp.text))
    return token_obj['content']


def create_token(service, token_name):
    pt = get_proxy_ticket(service)
    new_token = {'name': token_name}
    url = '{}/api/v1/accessToken?ticket={}'.format(service, pt)
    logging.info(url)

    resp = requests.post(url, data=json.dumps(new_token), verify=False)
    try:
        token = json.loads(resp.text)
    except Exception as e:
        raise Exception('[CAS] Error response when creating token: ' + str(resp.text))
    return token['content']
