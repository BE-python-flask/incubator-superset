import logging
import requests

from .access_token import guardian_server, get_token


def download_keytab(username, keytab_file, token):
    target_service = guardian_server()
    # if not token:
    #     token = get_token(username)
    url = '{}/api/v1/users/{}/keytab?guardian_access_token={}'\
        .format(target_service, username, token)
    logging.info('[CAS] Downloading keytab file to {}'.format(keytab_file))
    try:
        resp = requests.get(url, verify=False)
        with open(keytab_file, 'wb+') as f:
            f.write(resp.content)
    except Exception as e:
        raise Exception('[CAS] Error response when downloading keytab: ' + str(e))
