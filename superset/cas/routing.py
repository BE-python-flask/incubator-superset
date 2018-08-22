import flask
import logging
import requests
from xmltodict import parse
from flask import current_app, Response, g, request

from . import keys
from .access_token import get_token
from .cas_urls import (
    create_cas_login_url, create_cas_logout_url, create_cas_validate_url,
    create_cas_callback_url
)
from .proxy_ticket import get_proxy_ticket
from .cas_session import cas_session


try:
    from urllib import urlopen, urlprase
except ImportError:
    from urllib.request import urlopen
    from urllib.parse import urlparse

blueprint = flask.Blueprint('cas', __name__)


@blueprint.route('/login/', methods=['GET', 'POST'])
def login():
    """
    This route has two purposes. First, it is used by the user
    to login. Second, it is used by the CAS to respond with the
    `ticket` after the user logs in successfully.

    When the user accesses this url, they are redirected to the CAS
    to login. If the login was successful, the CAS will respond to this
    route with the ticket in the url. The ticket is then validated.
    If validation was successful the logged in username is saved in
    the user's session under the key `CAS_USERNAME_SESSION_KEY` and
    the user's attributes are saved under the key
    'CAS_USERNAME_ATTRIBUTE_KEY'
    """
    # Single Logout
    if request.form.get('logoutRequest'):
        logout_request = parse(request.form['logoutRequest'])
        ticket = logout_request.get('samlp:LogoutRequest', {}).get('samlp:SessionIndex')
        if cas_session.is_service_ticket(ticket):
            logging.info('[CAS] Handle logout request. Logout service ticket: {}...'
                         .format(ticket[:20]))
            cas_session.logout_st(ticket)
        elif cas_session.is_proxy_grant_ticket(ticket):
            logging.info('[CAS] Handle logout request. Destory proxy grant ticket: {}...'
                         .format(ticket[:20]))
            cas_session.destroy(ticket)
        return Response('[CAS] Handled the logout request.')

    redirect_url = create_cas_login_url(
        current_app.config[keys.CAS_SERVER],
        current_app.config[keys.CAS_LOGIN_ROUTE],
        flask.url_for('.login', _external=True))

    if 'ticket' in flask.request.args:
        flask.session[keys.CAS_SERVICE_TICKET] = flask.request.args['ticket']

    ticket = flask.session.get(keys.CAS_SERVICE_TICKET, None)
    if ticket is not None and ticket != keys.CAS_FAKE_SERVICE_TICKET:
        if validate(ticket):
            if keys.CAS_AFTER_LOGIN_SESSION_URL in flask.session:
                redirect_url = flask.session.pop(keys.CAS_AFTER_LOGIN_SESSION_URL)
            else:
                redirect_url = current_app.config[keys.CAS_AFTER_LOGIN]
        else:
            del flask.session[keys.CAS_SERVICE_TICKET]

    logging.info('[CAS] Redirecting to: {0}'.format(redirect_url))
    return flask.redirect(redirect_url)


@blueprint.route('/logout/')
def logout():
    """
    When the user accesses this route they are logged out.
    """
    if keys.CAS_USERNAME in flask.session:
        del flask.session[keys.CAS_USERNAME]

    if keys.CAS_SERVICE_TICKET in flask.session:
        del flask.session[keys.CAS_SERVICE_TICKET]

    if keys.CAS_PGTIOU in flask.session:
        del flask.session[keys.CAS_PGTIOU]

    if current_app.config[keys.CAS_AFTER_LOGOUT] is not None:
        redirect_url = create_cas_logout_url(
            current_app.config[keys.CAS_SERVER],
            current_app.config[keys.CAS_LOGOUT_ROUTE],
            current_app.config[keys.CAS_AFTER_LOGOUT])
    else:
        redirect_url = create_cas_logout_url(
            current_app.config[keys.CAS_SERVER],
            current_app.config[keys.CAS_LOGOUT_ROUTE],
            url_root())

    logging.info('[CAS] Redirecting to: {0}'.format(redirect_url))
    return flask.redirect(redirect_url)


def validate(ticket):
    """
    Will attempt to validate the ticket. If validation fails, then False
    is returned. If validation is successful, then True is returned
    and the validated username is saved in the session under the
    key `CAS_USERNAME_SESSION_KEY` while tha validated attributes dictionary
    is saved under the key 'CAS_ATTRIBUTES_SESSION_KEY'.
    """
    logging.info("[CAS] Validating service ticket {0}".format(ticket))

    cas_callback_url = create_cas_callback_url(
        url_root(),
        current_app.config[keys.CAS_PROXY_CALLBACK_ROUTE])
    cas_validate_url = create_cas_validate_url(
        current_app.config[keys.CAS_SERVER],
        current_app.config[keys.CAS_SERVICE_VALIDATE_ROUTE],
        service=flask.url_for('.login', _external=True),
        ticket=ticket,
        renew=None,
        pgtUrl=cas_callback_url)

    logging.info("[CAS] Making GET request to {0}".format(cas_validate_url))

    xml_from = {}
    isValid = False
    try:
        xmldump = urlopen(cas_validate_url).read().strip().decode('utf8', 'ignore')
        xml_from = parse(xmldump)
        isValid = True \
            if "cas:authenticationSuccess" in xml_from["cas:serviceResponse"] else False
    except ValueError:
        logging.error("[CAS] CAS returned unexpected result")

    if isValid:
        logging.info("[CAS] Valid Service Ticket: {}...".format(ticket[:20]))
        xml_from = xml_from["cas:serviceResponse"]["cas:authenticationSuccess"]
        username = xml_from["cas:user"]
        pgtiou = xml_from["cas:proxyGrantingTicket"]

        # Deprecated attributes
        # attributes = xml_from.get("cas:attributes", {})
        # if "cas:memberOf" in attributes:
        #     attributes["cas:memberOf"] = \
        #         attributes["cas:memberOf"].lstrip('[').rstrip(']').split(',')
        #     for group_number in range(0, len(attributes['cas:memberOf'])):
        #         attributes['cas:memberOf'][group_number] = \
        #             attributes['cas:memberOf'][group_number].lstrip(' ').rstrip(' ')

        flask.session[keys.CAS_USERNAME] = username
        flask.session[keys.CAS_SERVICE_TICKET] = ticket
        flask.session[keys.CAS_PGTIOU] = pgtiou
        cas_session.record(ticket, username, clear_logout=True)
    else:
        logging.info("[CAS] Invalid Service Ticket: {}...".format(ticket[:20]))

    return isValid


@blueprint.route('/proxyCallback/')
def pgt_callback():
    """
    Will attempt to be called back by CAS server to get PGT.
    This url should be added to '/serviceValidate' as parameter 'pgtUrl'.
    """
    pgtid = flask.request.args.get('pgtId')
    pgtiou = flask.request.args.get('pgtIou')
    if not pgtid:
        logging.info('[CAS] Not received pgtiou and pgt')
    else:
        logging.info('[CAS] Store pgtiou: {}... and pgt: {}...'
                     .format(pgtiou[:20], pgtid[:20]))
        cas_session.record(pgtiou, pgtid)
    return Response()


@blueprint.route('/test_proxy_ticket/')
def test_proxy_ticket():
    target_service = 'http://172.16.1.190:8380'
    target_service = flask.request.args.get('targetService', target_service)
    pt = get_proxy_ticket(target_service)
    if not pt:
        return Response('Failed to get proxy ticket')
    url = '{}?ticket={}'.format(target_service, pt)
    resp = requests.get(url)
    logging.info(url)
    return Response(resp.text)


@blueprint.route('/test_token/')
def test_token():
    token_name = request.args.get('token_name', 'pilot-token')
    token = get_token(g.user.username, token_name)
    return Response(token)


def url_root():
    u = urlparse(flask.url_for('.login', _external=True))
    return '{}://{}'.format(u.scheme, u.netloc)
