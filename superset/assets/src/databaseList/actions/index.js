import fetch from 'isomorphic-fetch';
import {getPublishConnectionUrl, isCorrectConnection} from '../utils';
import {renderLoadingModal, renderGlobalErrorMsg, PILOT_PREFIX} from '../../utils/utils';
import {always, json, callbackHandler} from '../../global.jsx';

export const actionTypes = {
    selectType: 'SELECT_TYPE',
    navigateTo: 'CHANGE_PAGE_NUMBER',
    changePageSize: 'CHANGE_PAGE_SIZE',

    selectRows: 'SELECT_ROWS',
    clearRows: 'CLEAR_ROWS',
    search: 'SEARCH',

    sendRequest: 'SEND_REQUEST',
    receiveData: 'RECEIVE_DATA',
    invalidateCondition: 'INVALIDATE_CONDITION',

    //for popup only
    setPopupTitle: 'SET_POPUP_TITLE',
    setPopupParam: 'SET_POPUP_PARAM',
    clearPopupParams: 'CLEAR_POPUP_PARAMS',
    changePopupStatus: 'CHNAGE_POPUP_STATUS',

    receiveConnectionNames: 'RECEIVE_CONNECTION_NAMES',
    switchFetchingState: 'SWITCH_FETCHING_STATE'
}

export const connectionTypes = {
    inceptor: 'INCEPTOR',
    hdfs: 'HDFS',
    mysql: 'MYSQL',
    oracle: "ORACLE",
    mssql: "MSSQL",
    database: "DATABASE"
};

const origin = window.location.origin;
const baseURL = origin + '/database/';
const connBaseURL = origin + '/connection/';
const INCEPTORConnectionBaseURL = origin + '/database/';
const HDFSConnectionBaseURL = origin + '/hdfsconnection/';

const getParamDB = (database) => {
    let db = {};
    let connectionType = (database.connectionType || database.backend);
    if (isCorrectConnection(connectionType, connectionTypes)) {
        db.database_name = database.database_name;
        db.sqlalchemy_uri = database.sqlalchemy_uri;
        db.description = database.description;
        db.args = database.databaseArgs;
        db.database_type = connectionType;
    } else {
        db = {
            connection_name: database.connection_name,
            description: database.description,
            httpfs: database.httpfs,
            database_id: database.database_id
        }
    }

    return db;
};

/**
@deprecated
*/
export function invalidateCondition(condition) {
    return {
        type: actionTypes.invalidateCondition,
        condition,
    };
}

export function selectType (type) {
    return {
        type: actionTypes.selectType,
        tableType: type
    }
}

export function navigateTo (pageNumber) {
    return {
        type: actionTypes.navigateTo,
        pageNumber: pageNumber
    }
}

export function changePageSize (pageSize) {
    return {
        type: actionTypes.changePageSize,
        pageSize: pageSize
    }
}

export function selectRows (selectedRowKeys, connToBeDeleted, selectedRowNames) {
    return {
        type: actionTypes.selectRows,
        selectedRowKeys,
        connToBeDeleted,
        selectedRowNames
    }
}

export function clearRows () {
    return {
        type: actionTypes.clearRows
    }
}

export function testConnection(database, callback) {
    return (dispatch, getState) => {
        dispatch(switchFetchingState(true));
        const URL = origin + PILOT_PREFIX + 'testconn/';
        return fetch(URL, {
            credentials: 'include',
            method: 'POST',
            body: JSON.stringify(database)
        }).then(always).then(json).then(
            response => {
                callbackHandler(response, callback);
                dispatch(switchFetchingState(false));
            }
        );
    }
}

export function testHDFSConnection(link, callback) {
    return (dispatch, getState) => {
        dispatch(switchFetchingState(true));
        const URL = HDFSConnectionBaseURL + 'test/?httpfs=' + link;
        return fetch(URL, {
            credentials: 'include',
            method: 'GET'
        }).then(always).then(json).then(
            response => {
                callbackHandler(response, callback);
                dispatch(switchFetchingState(false));
            }
        );
    }
}

export function fetchInceptorConnectAdd(inceptor, callback) {

    return (dispatch, getState) => {
        dispatch(switchFetchingState(true));
        const URL = baseURL + 'add/';
        return fetch(URL, {
            credentials: 'include',
            method: 'POST',
            body: JSON.stringify(inceptor)
        }).then(always).then(json).then(
            response => {
                callbackHandler(response, callback);
                dispatch(switchFetchingState(false));
                if(response.status === 200) {
                    dispatch(fetchIfNeeded());
                }
            }
        );
    }
}

export function fetchHDFSConnectAdd(hdfs, callback) {

    return (dispatch, getState) => {
        dispatch(switchFetchingState(true));
        const URL = HDFSConnectionBaseURL + 'add/';
        return fetch(URL, {
            credentials: 'include',
            method: 'POST',
            body: JSON.stringify(hdfs)
        }).then(always).then(json).then(
            response => {
                callbackHandler(response, callback);
                dispatch(switchFetchingState(false));
                if(response.status === 200) {
                    dispatch(fetchIfNeeded());
                }
            }
        );
    }
}

export function switchFetchingState(isFetching) {
    const loadingModal = renderLoadingModal();
    if(isFetching) {
        loadingModal.show();
    }else {
        loadingModal.hide();
    }
    return {
        type: actionTypes.switchFetchingState,
        isFetching: isFetching
    }
}

export function applyDelete (callback) {
    return (dispatch, getState) => {
        const URL = connBaseURL + 'muldelete/';
        const connToBeDeleted = getState().paramOfDelete.connToBeDeleted;
        dispatch(switchFetchingState(true));
        return fetch(URL, {
            credentials: 'include',
            method: 'post',
            body: JSON.stringify(connToBeDeleted)
        }).then(always).then(json).then(
            response => {
                callbackHandler(response, callback);
                dispatch(switchFetchingState(false));
                if (response.status === 200) {
                    dispatch(fetchIfNeeded(getState().condition));
                }
            }
        );
    }
}

export function fetchConnectDelInfo(connection, callback) {
    let baseUrl = INCEPTORConnectionBaseURL;
    if(connection.connection_type === connectionTypes.hdfs) {
        baseUrl = HDFSConnectionBaseURL;
    }
    const url = baseUrl + "delete_info/" + connection.id;
    return dispatch => {
        dispatch(switchFetchingState(true));
        return fetch(url, {
            credentials: "same-origin",
        }).then(always).then(json).then(
            response => {
                callbackHandler(response, callback);
                dispatch(switchFetchingState(false));
            }
        );
    }
}

export function fetchConnectDelMulInfo(callback) {
    return (dispatch, getState) => {
        dispatch(switchFetchingState(true));
        const url = connBaseURL + 'muldelete_info/';
        const connToBeDeleted = getState().paramOfDelete.connToBeDeleted;
        return fetch(url, {
            credentials: 'include',
            method: 'post',
            body: JSON.stringify(connToBeDeleted)
        }).then(always).then(json).then(
            response => {
                callbackHandler(response, callback);
                dispatch(switchFetchingState(false));
            }
        );
    }
}

const getURLBase = (type, subfix) => {
    if (!type) return;
    let URL = '';
    switch (type.toUpperCase()) {
        case connectionTypes.hdfs:
            URL = HDFSConnectionBaseURL;
            break;
        case connectionTypes.inceptor:
            URL = INCEPTORConnectionBaseURL;
            break;
        case connectionTypes.mysql:
            URL = INCEPTORConnectionBaseURL;
            break;
        case connectionTypes.oracle:
            URL = INCEPTORConnectionBaseURL;
            break;
        case connectionTypes.mssql:
            URL = INCEPTORConnectionBaseURL;
            break;
        default:
            break;
    }
    return URL + subfix;
}

export function fetchDBDetail(record, callback) {
    const type = record.connection_type;

    return dispatch => {
        const URL = getURLBase(type, 'show/') + record.id;
        return fetch(URL, {
            credentials: 'include',
            method: 'GET'
        }).then(always).then(json).then(
            response => {
                callbackHandler(response, callback);
            }
        );
    }
}

export function fetchUpdateConnection(database, callback) {

    return (dispatch, getState) => {
        const URL = getURLBase(database.connectionType, 'edit/') + database.id + '/';
        const db = getParamDB(database);
        dispatch(switchFetchingState(true));
        return fetch(URL, {
            credentials: 'include',
            method: 'POST',
            body: JSON.stringify(db)
        }).then(always).then(json).then(
            response => {
                callbackHandler(response, callback);
                dispatch(switchFetchingState(false));
                if(response.status === 200) {
                    dispatch(fetchIfNeeded());
                }
            }
        );
    }
}

export function search (filter) {
    return {
        type: actionTypes.search,
        filter: filter
    };
}

function sendRequest (condition) {
    return {
        type: actionTypes.sendRequest,
        condition
    };
}

function receiveData (condition, json) {
    return {
        type: actionTypes.receiveData,
        condition,
        response: json,
        receivedAt: Date.now()
    };
}

export function fetchTypes (callback) {
    return (dispatch, getState) => {
        const URL = connBaseURL + 'connection_types';

        return fetch(URL, {
            credentials: 'include',
            method: 'GET'
        }).then(always).then(json).then(
            response => {
                callbackHandler(response, callback);
            }
        );
    };
}

function applyFetch (condition) {
    return (dispatch, getState) => {
        dispatch(sendRequest(condition));

        const URL = connBaseURL + 'listdata/?' +
            (condition.page? 'page=' + (+condition.page-1) : '') +
            (condition.pageSize? '&page_size=' + condition.pageSize : '') +
            (condition.orderColumn? '&order_column=' + condition.orderColumn : '') +
            (condition.orderDirection? '&order_direction=' + condition.orderDirection : '') +
            (condition.filter? '&filter=' + condition.filter : '') +
            (condition.tableType&&condition.tableType!=='all'? '&table_type=' + condition.tableType : '');

        return fetch(URL, {
            credentials: 'include',
            method: 'GET'
        }).then(always).then(json).then(
            response => {
                fetchListHanlder(condition, response, dispatch);
            }
        );

    };
}

function fetchListHanlder(condition, response, dispatch) {
    if(response.status === 200) {
        const json = response.data;
        if(!json.data)
            return json;
        else {
            json.data.map(function(obj, index){
                obj.iconClass = (obj.dataset_type == 'hdfs_folder'? 'HDFS' : obj.dataset_type == 'Inceptor'?'Inceptor' : 'upload');
                obj.elementId = index+1;
            });
        }
        dispatch(receiveData(condition, json));
    }else {
        renderGlobalErrorMsg(response.message);
    }
}

function shouldFetch (state, condition) {
    return true;
}

export function fetchIfNeeded (condition) {

    return (dispatch, getState) => {
        const conditionInside = condition || getState().condition;
        if (shouldFetch(getState(), conditionInside)) {
            dispatch(clearRows());
            return dispatch(applyFetch(conditionInside));
        }
        return null;
    };
}

export const fetchConnectionNames = (callback) => {
    return (dispatch, getState) => {
        const URL = `${baseURL}listdata/?page_size=1000&database_type=inceptor`;

        return fetch(URL, {
            credentials: 'include',
            method: 'get'
        }).then(always).then(json).then(
            response => {
                callbackHandler(response, callback);
            }
        );
    }
};