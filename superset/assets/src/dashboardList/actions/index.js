/**
 * Created by haitao on 17-5-18.
 */
import fetch from 'isomorphic-fetch';
import intl from "react-intl-universal";
import { initJQueryAjax } from '../../modules/utils';

import { renderLoadingModal, renderGlobalErrorMsg, PILOT_PREFIX } from '../../utils/utils';
import { getNewDashboard, getSelectedSlices } from '../../utils/common';
import { always, json, callbackHandler } from '../../global';

export const REQUEST_POSTS = 'REQUEST_POSTS';
export const RECEIVE_POSTS = 'RECEIVE_POSTS';
export const setBinary = 'set_binary';
export const setImportParams = 'set_import_params';

export const REQUEST_DASHBOARD_DETAIL = 'REQUEST_DASHBOARD_DETAIL';
export const RECEIVE_DASHBOARD_DETAIL = 'RECEIVE_DASHBOARD_DETAIL';

export const CONFIG_PARAMS = {
    SET_KEYWORD: 'SET_KEYWORD',
    SHOW_TYPE: 'SHOW_TYPE',
    PAGE_NUMBER: 'PAGE_NUMBER',
    PAGE_SIZE: 'PAGE_SIZE',
    SELECTED_ROWS: 'SELECTED_ROWS',
    CLEAR_ROWS: 'CLEAR_ROWS',
    VIEW_MODE: 'VIEW_MODE',
    TABLE_LOADING: 'TABLE_LOADING',
    SWITCH_FETCHING_STATE: 'SWITCH_FETCHING_STATE'
};

const getCSRFHeader = function() {
    var myHeaders = new Headers();
    const token = $('input#csrf_token').val();
    // myHeaders.append('X-CSRF-TOKEN', token);
    myHeaders.append('X-CSRFToken', token);
    return myHeaders;
};

const handler = (response, dispatch) => {
    if (response.status === 200) {
        dispatch(receivePosts(response.data));
    } else {
        renderGlobalErrorMsg(response.message);
    }
};

export function requestPosts() {
    return {
        type: REQUEST_POSTS
    }
}

export function receivePosts(json) {
    return {
        type: RECEIVE_POSTS,
        posts: json
    }
}

export function requestDashboardDetail() {
    return {
        type: RECEIVE_DASHBOARD_DETAIL
    }
}

export function receiveDashboardDetail(json) {
    return {
        type: RECEIVE_DASHBOARD_DETAIL,
        detail: json
    }
}

export function setKeyword(keyword) {
    return {
        type: CONFIG_PARAMS.SET_KEYWORD,
        keyword: keyword
    }
}

export function setShowType(typeName) {
    return {
        type: CONFIG_PARAMS.SHOW_TYPE,
        typeName: typeName
    }
}

export function setPageNumber(pageNumber) {
    return {
        type: CONFIG_PARAMS.PAGE_NUMBER,
        pageNumber: pageNumber
    }
}

export function setPageSize(pageSize) {
    return {
        type: CONFIG_PARAMS.PAGE_SIZE,
        pageSize: pageSize
    }
}

export function setSelectedRow(selectedRowKeys, selectedRowNames) {
    return {
        type: CONFIG_PARAMS.SELECTED_ROWS,
        selectedRowKeys: selectedRowKeys,
        selectedRowNames: selectedRowNames
    }
}

export function clearRows() {
    return {
        type: CONFIG_PARAMS.CLEAR_ROWS,
        selectedRowKeys: [],
        selectedRowNames: []
    }
}

export function appendRow(dashboard, selectedRowKeys, selectedRowNames) {
    return {
        type: CONFIG_PARAMS.SELECTED_ROWS,
        selectedRowKeys: getSelectedRows(dashboard, selectedRowKeys, selectedRowNames, 'append').selectedRowKeys,
        selectedRowNames: getSelectedRows(dashboard, selectedRowKeys, selectedRowNames, 'append').selectedRowNames
    }
}

export function removeRow(dashboard, selectedRowKeys, selectedRowNames) {
    return {
        type: CONFIG_PARAMS.SELECTED_ROWS,
        selectedRowKeys: getSelectedRows(dashboard, selectedRowKeys, selectedRowNames, 'remove').selectedRowKeys,
        selectedRowNames: getSelectedRows(dashboard, selectedRowKeys, selectedRowNames, 'remove').selectedRowNames
    }
}

export function setViewMode(viewMode) {
    return {
        type: CONFIG_PARAMS.VIEW_MODE,
        viewMode: viewMode
    }
}

export function setTableLoadingStatus(loading) {
    return {
        type: CONFIG_PARAMS.TABLE_LOADING,
        tableLoading: loading
    }
}

export function switchFetchingState(isFetching) {
    const loadingModal = renderLoadingModal();
    if (isFetching) {
        loadingModal.show();
    } else {
        loadingModal.hide();
    }
    return {
        type: CONFIG_PARAMS.SWITCH_FETCHING_STATE,
        isFetching: isFetching
    }
}

export function fetchDashboardDelete(dashboardId, callback) {
    const url = window.location.origin + "/dashboard/delete/" + dashboardId;
    return dispatch => {
        return fetch(url, {
            credentials: "same-origin"
        }).then(always).then(json).then(
            response => {
                callbackHandler(response, callback);
                if (response.status === 200) {
                    dispatch(fetchPosts());
                }
            }
        );
    }
}

export function fetchDashbaordDelInfo(dashboardId, callback) {
    const url = window.location.origin + "/dashboard/delete_info/" + dashboardId;
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

export function fetchDashboardDeleteMul(callback) {
    const url = window.location.origin + "/dashboard/muldelete/";
    return (dispatch, getState) => {
        const selectedRowKeys = getState().configs.selectedRowKeys;
        let data = {
            selectedRowKeys: selectedRowKeys
        };
        return fetch(url, {
            credentials: "same-origin",
            method: "POST",
            body: JSON.stringify(data)
        }).then(always).then(json).then(
            response => {
                callbackHandler(response, callback);
                if (response.status === 200) {
                    dispatch(fetchPosts());
                }
            }
        );
    }
}
export function setBinaryFile(binaryFile) {
    return {
        type: setBinary,
        binaryFile: binaryFile
    }
}
export function setupImportParams(paramData) {
    return {
        type: setImportParams,
        paramData: paramData
    }
}

// fetch if the imported file has duplicated dashboard
export function fetchBeforeImport(callback) {
    return (dispatch, getState) => {

        const importParams = getState().importParams;
        const binaryFile = importParams.binaryFile;
        const url = window.location.origin + "/dashboard/before_import/";
        
        // can not handle file upload without plugin of jq
/*      initJQueryAjax();
        $.ajax({
            type: "POST",
            //dataType:'json',
            url: url,
            data: {
                file: binaryFile
            }
        }).done(function(res) {
            callbackHandler(response, callback);
        }); */

        return fetch(url, {
            credentials: 'include',
            method: "POST",
            body: binaryFile,
            cache: 'no-store',

            // mode: "cors" // https://developer.mozilla.org/en-US/docs/Web/API/Fetch_API/Using_Fetch
            headers: getCSRFHeader()
        }).then(always).then(json).then(
            response => {
                callbackHandler(response, callback);
            }
        );
    }
}

export function fetchDashboardExport(callback) {
    return (dispatch, getState) => {
        const selectedRowKeys = getState().configs.selectedRowKeys;

        if (selectedRowKeys.length > 0) {
            const url = window.location.origin + "/dashboard/export/?ids=" + selectedRowKeys;
            const sentTime = new Date();
            const name = 'dashboard_' + sentTime.getUTCDate() + sentTime.getUTCMilliseconds() + '.pickle';

            window.open(url, '_blank', 'toolbar=yes, location=yes, status=yes, menubar=yes, scrollbars=yes');

        } else {
            renderGlobalErrorMsg(intl.get('DASHBOARD.SELECT_SOMEONE'));
        }
    }
}

export function fetchDashboardImport(callback) {

    return (dispatch, getState) => {
        let url = window.location.origin + "/dashboard/import/";
        const state = getState();
        const importParams = state.importParams;
        const paramData = {
            ...importParams.paramData
        };
        const binaryFile = importParams.binaryFile;

        let parent;
        let child;
        for (let i in paramData) {
            parent = paramData[i];
            for (let j in parent.names) {
                child = parent.names[j];
                delete child.can_overwrite;
                delete child.name;
                parent.names[j] = child;
            }
            paramData[i] = paramData[i].names;
        }
        // const search = $.param(paramData);
        url = url + '?param=' + JSON.stringify(paramData);

        return fetch(url, {
            credentials: 'include',
            method: 'POST',
            body: binaryFile,

            headers: getCSRFHeader()
        }).then(always).then(json).then(
            response => {
                callbackHandler(response, callback);
                dispatch(switchFetchingState(false));
            }
        )
    }
}

export function fetchDashboardMulDelInfo(callback) {
    const url = window.location.origin + "/dashboard/muldelete_info/";
    return (dispatch, getState) => {
        const selectedRowKeys = getState().configs.selectedRowKeys;
        let data = {
            selectedRowKeys: selectedRowKeys
        };
        dispatch(switchFetchingState(true));
        return fetch(url, {
            credentials: "same-origin",
            method: "POST",
            body: JSON.stringify(data)
        }).then(always).then(json).then(
            response => {
                callbackHandler(response, callback);
                dispatch(switchFetchingState(false));
            }
        );
    }
}

export function fetchAvailableSlices(callback) {
    const url = window.location.origin + "/slice/listdata/?page_size=1000";
    return fetch(url, {
        credentials: "same-origin"
    }).then(always).then(json).then(
        response => {
            callbackHandler(response, callback);
        }
    );
}

export function fetchUpdateDashboard(state, dashboard, callback) {
    const url = window.location.origin + "/dashboard/edit/" + dashboard.id + '/';
    const newDashboard = getNewDashboard(dashboard, state.selectedSliceObjs, state.availableSlices);
    return dispatch => {
        return fetch(url, {
            credentials: "same-origin",
            method: "POST",
            body: JSON.stringify(newDashboard)
        }).then(always).then(json).then(
            response => {
                callbackHandler(response, callback);
                dispatch(switchFetchingState(false));
                if (response.status === 200) {
                    dispatch(fetchPosts());
                }
            }
        );
    }
}

export function fetchAddDashboard(state, availableSlices, callback) {
    const url = window.location.origin + "/dashboard/add/";
    const dashboard = getNewDashboard(state.dashboard, state.selectedSliceObjs, availableSlices);
    return dispatch => {
        dispatch(switchFetchingState(true));
        return fetch(url, {
            credentials: "same-origin",
            method: "POST",
            body: JSON.stringify(dashboard)
        }).then(always).then(json).then(
            response => {
                callbackHandler(response, callback);
                dispatch(switchFetchingState(false));
                if (response.status === 200) {
                    dispatch(fetchPosts());
                }
            }
        );
    }
}

export function fetchStateChange(record, callback, type) {
    const url = getStateChangeUrl(record, type);
    return dispatch => {
        dispatch(switchFetchingState(true));
        return fetch(url, {
            credentials: "same-origin",
        }).then(always).then(json).then(
            response => {
                callbackHandler(response, callback);
                dispatch(switchFetchingState(false));
                dispatch(fetchPosts());
            }
        );
    }
}

export function fetchDashboardDetail(dashboardId, callback) {
    const url = window.location.origin + "/dashboard/show/" + dashboardId;
    return dispatch => {
        return fetch(url, {
            credentials: "same-origin",
        }).then(always).then(json).then(
            response => {
                callbackHandler(response, callback);
            }
        );
    }
}

export function fetchPosts() {
    return (dispatch, getState) => {
        dispatch(requestPosts());
        const url = getDashboardListUrl(getState());
        return fetch(url, {
            credentials: "same-origin"
        }).then(always).then(json).then(
            response => {
                handler(response, dispatch);
                dispatch(clearRows());
            }
        );
    }
}

function getDashboardListUrl(state) {
    let url = window.location.origin + "/dashboard/listdata/?page=" + (state.configs.pageNumber - 1) +
    "&page_size=" + state.configs.pageSize + "&filter=" + state.configs.keyword;
    if (state.configs.type === "show_favorite") {
        url += "&only_favorite=1";
    }
    return url;
}

function getStateChangeUrl(record, type) {
    if (type === "favorite") {
        let url_favorite = window.location.origin + PILOT_PREFIX + "favstar/Dashboard/" + record.id;
        if (record.favorite) {
            url_favorite += "/unselect";
        } else {
            url_favorite += "/select";
        }
        return url_favorite;
    } else if (type === "publish") {
        let url_publish = window.location.origin + PILOT_PREFIX + "release/dashboard/";
        if (record.online) {
            url_publish += "offline/" + record.id;
        } else {
            url_publish += "online/" + record.id;
        }
        return url_publish;
    }
}

function getSelectedRows(dashboard, selectedRowKeys, selectedRowNames, type) {
    let row = {};
    if (type === "append") {
        let existed = false;
        selectedRowKeys.map((key) => {
            if (key === dashboard.id) {
                existed = true;
            }
        });
        if (!existed) {
            selectedRowKeys.push(dashboard.id);
            selectedRowNames.push(dashboard.name);
        }
    } else if (type === "remove") {
        selectedRowKeys.map((key, index) => {
            if (key === dashboard.id) {
                selectedRowKeys.splice(index, 1);
                selectedRowNames.splice(index, 1);
            }
        });
    }
    row.selectedRowKeys = selectedRowKeys;
    row.selectedRowNames = selectedRowNames;
    return row;
}

