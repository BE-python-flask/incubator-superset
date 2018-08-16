import fetch from 'isomorphic-fetch';

import { constructFileBrowserData, appendTreeChildren, findTreeNode } from '../../tableList/module';
import { getSelectedPath } from '../module';
import { renderLoadingModal, renderGlobalErrorMsg } from './../../utils/utils'

export const actionTypes = {
    sendRequest: 'SEND_REQUEST',
    receiveData: 'RECEIVE_DATA',
    search: 'SEARCH',
    setSelectedRows: 'SET_SELECTED_ROWS',
    switchFetchingStatus: 'SWITCH_FETCHING_STATUS',
    navigateTo: 'NAVIGATE_TO',
    changePageSize: 'CHANGE_PAGE_SIZE'
};

export const popupActions = {
    setPopupParam: 'SET_POPUP_PARAM',
    popupChangeStatus: 'POPUP_CHANGE_STATUS'
};

export const popupNormalActions = {
    setPopupParams: 'SET_POPUP_NORMAL_PARAMS',
    setPopupParam: 'SET_POPUP_NORMAL_PARAM',
    popupChangeStatus: 'POPUP_NORMAL_CHANGE_STATUS',
    setPermData: 'SET_PERM_DATA',
    setPermMode: 'SET_PERM_MODE',
    setHDFSPath: 'SET_HDFS_PATH',
    setRecursivePerm: 'SET_RECURSIVE_PERM',
    setTreeData: 'SET_TREE_DATA',
};

const ORIGIN = window.location.origin;
const baseURL = `${ORIGIN}/hdfs/`;

export const CONSTANT = {
    mkdir: 'mkdir',
    touch: 'touch',

    move: 'move',
    copy: 'copy',
    download: 'download',
    auth: 'auth',
    upload: 'upload',
    remove: 'remove',
    noSelect: 'noSelect',

    baseURL: baseURL
};

const callbackHandler = (success, response, callback) => {
    callback && callback(success, response);
};
const always = (response) => {
    return Promise.resolve(response);
};
const json = (response) => {
    return response.json();
};
const popupHandler = (response, popupNormalParam, dispatch, condition) => {
    let obj = {};
    if (response.status === 200) {
        obj = {
            ...popupNormalParam,
            status: 'none',
            alertStatus: 'none',
            alertMsg: response.message,
            alertType: 'success'
        };
    } else {
        obj = {
            ...popupNormalParam,
            status: 'flex',
            alertStatus: '',
            alertMsg: response.message,
            alertType: 'error'
        };
    }
    dispatch(setPopupNormalParams(obj));
    dispatch(setSelectedRows([], [], []));
    dispatch(switchFetchingStatus(false));
    if (popupNormalParam.popupType !== "noSelect") {
        dispatch(fetchIfNeeded(condition));
    }
};

/**
@description: S-mock
*/

actionTypes.changePath = 'CHANGE_PATH';
export function changePath(path) {
    return {
        type: actionTypes.changePath,
        path
    }
}

export function fetchOperation(param) {
    let submit = argu => argu;
    return (dispatch, getState) => {
        const popupType = getState().popupNormalParam.popupType;

        switch (popupType) {
        case CONSTANT.move:
            submit = fetchMove;
            break;
        case CONSTANT.copy:
            submit = fetchCopy;
            break;
        case CONSTANT.auth:
            submit = fetchAuth;
            break;
        case CONSTANT.download:
            submit = fetchDownload;
            break;
        case CONSTANT.upload:
            submit = fetchUpload;
            break;
        case CONSTANT.mkdir:
            submit = fetchMakedir;
            break;
        case CONSTANT.touch:
            submit = fetchMakeFile;
            break;
        case CONSTANT.remove:
            submit = fetchRemove;
            break;
        case CONSTANT.noSelect:
            submit = fetchNoSelection;
            break;
        default:
            break;
        }
        return dispatch(submit(param));
    }
}

function fetchMove() {
    return (dispatch, getState) => {
        const popupNormalParam = getState().popupNormalParam;
        const condition = getState().condition;
        const path = getSelectedPath(condition.selectedRows);
        const dest_path = popupNormalParam.dest_path;
        const params = {
            path: path,
            dest_path: dest_path
        };
        const URL = baseURL + 'move/';
        dispatch(switchFetchingStatus(true));
        return fetch(URL, {
            credentials: 'include',
            method: 'POST',
            body: JSON.stringify(params)
        }).then(always).then(json).then(
            response => {
                popupHandler(response, popupNormalParam, dispatch, condition);
            }
        );
    }
}

function fetchCopy() {
    return (dispatch, getState) => {
        const popupNormalParam = getState().popupNormalParam;
        const condition = getState().condition;
        const path = getSelectedPath(condition.selectedRows);
        const dest_path = popupNormalParam.dest_path;
        const params = {
            path: path,
            dest_path: dest_path
        };
        const URL = baseURL + 'copy/';
        dispatch(switchFetchingStatus(true));
        return fetch(URL, {
            credentials: 'include',
            method: 'POST',
            body: JSON.stringify(params)
        })
            .then(always).then(json).then(
            response => {
                popupHandler(response, popupNormalParam, dispatch, condition);
            }
        );
    }
}

//no api but have the function
function fetchAuth() {
    return (dispatch, getState) => {
        const state = getState();
        const popupNormalParam = state.popupNormalParam;
        const params = {
            path: [popupNormalParam.path],
            mode: popupNormalParam.permMode,
            recursive: popupNormalParam.recursivePerm
        };
        const URL = baseURL + "chmod/";
        dispatch(switchFetchingStatus(true));
        return fetch(URL, {
            credentials: 'include',
            method: 'POST',
            body: JSON.stringify(params)
        }).then(always).then(json).then(
            response => {
                popupHandler(response, popupNormalParam, dispatch, state.condition);
            }
        );
    }
}


function fetchMakedir() {
    return (dispatch, getState) => {
        const popupNormalParam = getState().popupNormalParam;
        const path = popupNormalParam.path;
        const dir_name = popupNormalParam.dir_name;
        const URL = baseURL + 'mkdir/?path=' + path + '&dir_name=' + dir_name;
        dispatch(switchFetchingStatus(true));
        return fetch(URL, {
            credentials: 'include',
            method: 'GET'
        }).then(always).then(json).then(
            response => {
                popupHandler(response, popupNormalParam, dispatch, getState().condition);
            }
        );
    }
}

function fetchMakeFile() {
    return (dispatch, getState) => {
        const popupNormalParam = getState().popupNormalParam;
        const path = popupNormalParam.path;
        const filename = popupNormalParam.filename;
        const URL = baseURL + 'touch/?path=' + path + '&filename=' + filename;
        dispatch(switchFetchingStatus(true));
        return fetch(URL, {
            credentials: 'include',
            method: 'GET'
        }).then(always).then(json).then(
            response => {
                popupHandler(response, popupNormalParam, dispatch, getState().condition);
            }
        );
    }
}

function fetchRemove() {
    return (dispatch, getState) => {
        let state = getState();
        const selectedRows = state.condition.selectedRows;
        const popupNormalParam = state.popupNormalParam;
        let path = [];
        selectedRows.map((currentValue, index, array) => {
            path.push(currentValue.path);
        });
        const URL = baseURL + `remove/`;
        dispatch(switchFetchingStatus(true));
        return fetch(URL, {
            credentials: 'include',
            method: 'POST',
            body: JSON.stringify({
                path: path
            })
        }).then(always).then(json).then(
            response => {
                dispatch(navigateTo(1));
                state = getState();
                popupHandler(response, popupNormalParam, dispatch, state.condition);
            }
        );
    }
}

export function fetchDownload() {
    return (dispatch, getState) => {
        // TODO: file download in detaild page
        const state = getState();
        let URL = baseURL + `download/?`;
        const selectedRows = state.condition.selectedRows;

        if (selectedRows.length > 0) {
            const paths = {
                files: [],
                folders: []
            };

            selectedRows.forEach(item => {
                switch (item.type) {
                case 'dir':
                    paths.folders.push(item.path);
                    break;
                case 'file':
                    paths.files.push(item.path);
                    break;
                default:
                    console.log('no type');
                    return;
                }
            });

            paths.files.length > 0 && (URL += ('files=' + paths.files.toLocaleString()) + `&`);
            paths.folders.length > 0 && (URL += ('folders=' + paths.folders.toLocaleString()));
        } else {
            const fileReducer = state.fileReducer;
            const path = fileReducer.path;
            URL += ('files=' + path);
        }
        window.open(URL, '_blank', 'toolbar=yes, location=yes, status=yes, menubar=yes, scrollbars=yes');

    /*return fetch(URL, {
        credentials: 'include',
        method: 'POST',
        body: JSON.stringify({
            paths: paths
        })
    }).then(always).then(json).then(
    response => {
        if (response.status === 200) {
            let aLink = document.createElement('a');
            const data = response.data;
            let blob = new Blob([data], {
                type: 'plain/text',
                endings: 'native'
            });
            let url = window.URL.createObjectURL(blob);
            aLink.href = url;
            aLink.download = name;
            // aLink.click();     
            // not compatable with Firefox, changed to use below method:

            var evt = document.createEvent("MouseEvents");
            evt.initEvent("click", false, false);
            aLink.dispatchEvent(evt);
            window.URL.revokeObjectURL(url);
        } else {
            renderGlobalErrorMsg(response.message);
        }
        dispatch(switchFetchingStatus(false));
    }
    );*/
    }
}

popupNormalActions.fetchNoSelection = 'FETCH_NO_SELECTION';
function fetchNoSelection() {
    return (dispatch, getState) => {
        const popupNormalParam = getState().popupNormalParam;
        const response = {
            status: 200,
            message: ''
        };
        popupHandler(response, popupNormalParam, dispatch, getState().condition);
    }
}

export function setPermData(permData) {
    return {
        type: popupNormalActions.setPermData,
        permData: permData
    }
}

export function setPermMode(permMode) {
    return {
        type: popupNormalActions.setPermMode,
        permMode: permMode
    }
}

export function setRecursivePerm(isRecursive) {
    return {
        type: popupNormalActions.setRecursivePerm,
        recursivePerm: isRecursive
    }
}

export function setHDFSPath(path) {
    return {
        type: popupNormalActions.setHDFSPath,
        path: path
    }
}

//S: popup related actions
export function setPopupParam(param) {
    return {
        type: popupActions.setPopupParam,
        status: param.status,
        response: param.response
    }
}

export function navigateTo(pageNum) {
    return {
        type: actionTypes.navigateTo,
        pageNum: pageNum
    }
}

export function changePageSize(pageSize) {
    return {
        type: actionTypes.changePageSize,
        pageSize: pageSize
    }
}

/**
deprecated
*/
export function popupChangeStatus(param) {
    return {
        type: popupActions.popupChangeStatus,
        status: param
    }
}

//normal popup in operation
export function setPopupNormalParams(param) {
    return {
        type: popupNormalActions.setPopupParams,
        ...param
    }
}

//normal popup in operation
export function setPopupNormalParam(param) {
    return {
        type: popupNormalActions.setPopupParam,
        param
    }
}
export function popupNormalChangeStatus(param) {
    return {
        type: popupNormalActions.popupChangeStatus,
        status: param
    }
}
//E: popup related actions
/**
@description: S:operation functions here
*/
export function search(filter) {
    return {
        type: actionTypes.search,
        filter: filter
    };
}
/**
@description: E:operation functions here
*/

function sendRequest(condition) {
    return {
        type: actionTypes.sendRequest,
        condition,
    };
}

function receiveData(json, condition) {
    return {
        type: actionTypes.receiveData,
        condition,
        response: json,

        receivedAt: Date.now()
    };
}

popupNormalActions.requestLeafData = 'REQUEST_LEAF_DATA';
function requestLeafData(json) {
    return {
        type: popupNormalActions.requestLeafData,
        isFetching: true
    };
}
popupNormalActions.receiveLeafData = 'RECEIVE_LEAF_DATA';
function receiveLeafData(json) {
    console.log('in receiveLeafData:', json);
    return {
        type: popupNormalActions.receiveLeafData,
        treeData: json,
        isFetching: false
    };
}

popupNormalActions.swapResponse = 'SWAP_RESPONSE';
export function swapResponse(json) {
    return {
        type: popupNormalActions.swapResponse,
        response: json.response
    };
}

export function switchFetchingStatus(isFetching) {
    const loadingModal = renderLoadingModal();
    if (isFetching) {
        loadingModal.show();
    } else {
        loadingModal.hide();
    }
    return {
        type: actionTypes.switchFetchingStatus,
        isFetching: isFetching
    }
}

function applyFetch(condition) {
    return (dispatch, getState) => {
        dispatch(sendRequest(condition));
        dispatch(switchFetchingStatus(true));
        const URL = baseURL + `list/?` +
        (condition.path ? ('path=' + condition.path + '&') : '') +
        (condition.page_num ? ('page_num=' + condition.page_num + '&') : 'page_num=1&') +
        (condition.page_size ? ('page_size=' + condition.page_size) : 'page_size=10');

        const listDataMatch = json => {
            if (!json || !json.files) return json;
            json.files.map(function(obj, index, arr) {
                obj.key = index + 1;
            });
            return json;
        };
        return fetch(URL, {
            credentials: 'include',
            method: 'GET',
            mode: 'cors'
        }).then(always).then(json).then(
            response => {
                if (response.status === 200) {
                    const data = listDataMatch(response ? response.data : {});
                    dispatch(receiveData(data, condition));
                } else {
                    renderGlobalErrorMsg(response.message);
                }
                dispatch(switchFetchingStatus(false));
            }
        );
    };
}

function shouldFetch(state, condition) {
    return true;
}

export function fetchIfNeeded(condition) {
    return (dispatch, getState) => {
        if (shouldFetch(getState(), condition)) {
            return dispatch(applyFetch(condition));
        }
        return null;
    };
}

export function setSelectedRows(selectedRows, selectedRowKeys, selectedRowNames) {
    selectedRowNames && (selectedRowNames.length > 1) && selectedRowNames.forEach((name, idx) => {
        if (name === '".."' || name === '"."') {
            selectedRows = selectedRows.splice(idx, 1);
            selectedRowKeys = selectedRowKeys.sort((a, b) => (a - b)).splice(idx, 1);
            selectedRowNames = selectedRowNames.splice(idx, 1);
        }
    });

    return {
        type: actionTypes.setSelectedRows,
        selectedRows,
        selectedRowKeys,
        selectedRowNames
    }
}

actionTypes.giveDetail = 'GIVE_DETAIL';
export function giveDetail(detail) {
    return {
        type: actionTypes.giveDetail,
        detail
    }
}

actionTypes.setPreview = 'SET_PREVIEW';
export function setPreview(preview) {
    return {
        type: actionTypes.setPreview,
        preview
    }
}

export function fetchPreview() {
    return (dispatch, getState) => {
        const state = getState();

        const URL = baseURL + `preview/?path=${state.fileReducer.path}`;
        dispatch(switchFetchingStatus(true));
        return fetch(URL, {
            credentials: 'include',
            method: 'GET'
        }).then(always).then(json).then(
            response => {
                if (response.status === 200) {
                    dispatch(setPreview(response.data));
                } else if (response.status === 500) {
                    renderGlobalErrorMsg(response.message);
                }
                dispatch(switchFetchingStatus(false));
            }
        );
    }
}

export function fetchHDFSList(path, isFirst) {
    return (dispatch, getState) => {
        dispatch(switchFetchingStatus(true));
        const url = baseURL + 'list/?path=' + path + '&page_num=1&page_size=1000';
        return fetch(url, {
            credentials: 'include',
            method: 'GET'
        }).then(always).then(json).then(
            response => {
                if (response.status === 200) {
                    if (isFirst) {
                        dispatch(setTreeData(constructFileBrowserData(response.data)));
                    } else {
                        dispatch(setTreeData(appendTreeChildren(
                            path,
                            response.data,
                            JSON.parse(JSON.stringify(getState().popupNormalParam.treeData))
                        )));
                    }
                } else {
                    renderGlobalErrorMsg(response.message);
                }
                dispatch(switchFetchingStatus(false));
            }
        );
    }
}

export function setTreeData(treeData) {
    return {
        type: popupNormalActions.setTreeData,
        treeData: treeData
    }
}