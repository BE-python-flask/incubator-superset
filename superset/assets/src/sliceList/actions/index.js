import fetch from 'isomorphic-fetch';
import { renderLoadingModal, renderGlobalErrorMsg, PILOT_PREFIX } from '../../utils/utils'
import { always, json, callbackHandler } from '../../global.jsx';

export const SHOW_ALL = 'showAll';
export const SHOW_FAVORITE = 'showFavorite';

export const LISTS = {
    REQUEST_LISTS: 'REQUEST_LISTS',
    RECEIVE_LISTS: 'RECEIVE_LISTS',
    SWITCH_FETCHING_STATE: 'SWITCH_FETCHING_STATE'
};

export const DETAILS = {
    REQUEST_LISTS: 'REQUEST_DETAIL',
    RECEIVE_LISTS: 'RECEIVE_DETAIL'
};

export const CONDITION_PARAMS = {
    KEYWORD: 'KEYWORD',
    SHOW_TYPE: 'SHOW_TYPE',
    PAGE_NUMBER: 'PAGE_NUMBER',
    PAGE_SIZE: 'PAGE_SIZE',
    CLEAR_ROWS: 'CLEAR_ROWS',
    SELECTED_ROWS: 'SELECTED_ROWS',
    TABLE_LOADING: 'TABLE_LOADING'
};

const handler = (response, dispatch) => {
    if (response.status === 200) {
        dispatch(receiveLists(response.data));
    } else {
        renderGlobalErrorMsg(response.message);
    }
};

const baseURL = window.location.origin + '/slice/';

export function navigateTo(pageNumber) {
    return {
        type: CONDITION_PARAMS.PAGE_NUMBER,
        pageNumber: pageNumber
    }
}

export function setPageSize(pageSize) {
    return {
        type: CONDITION_PARAMS.PAGE_SIZE,
        pageSize: pageSize
    }
}

export function switchFavorite(typeName) {
    return {
        type: CONDITION_PARAMS.SHOW_TYPE,
        typeName: typeName
    }
}

export function requestLists() {
    return {
        type: LISTS.REQUEST_LISTS
    };
}

export function setKeyword(keyword) {
    return {
        type: CONDITION_PARAMS.KEYWORD,
        keyword: keyword
    }
}

export function setSelectedRows(selectedRowKeys, selectedRowNames) {
    return {
        type: CONDITION_PARAMS.SELECTED_ROWS,
        selectedRowKeys: selectedRowKeys,
        selectedRowNames: selectedRowNames
    }
}

export function setTableLoadingStatus(loading) {
    return {
        type: CONDITION_PARAMS.TABLE_LOADING,
        tableLoading: loading
    }
}

export function clearRows() {
    return {
        type: CONDITION_PARAMS.SELECTED_ROWS,
        selectedRowKeys: [],
        selectedRowNames: []
    }
}

export function receiveLists(json) {
    return {
        type: LISTS.RECEIVE_LISTS,
        lists: json
    };
}

export function fetchStateChange(record, callback, type) {
    const url = getStateChangeUrl(record, type);
    return dispatch => {
        dispatch(switchFetchingState(true));
        return fetch(url, {
            credentials: 'include',
            method: 'GET'
        }).then(always).then(json).then(
            response => {
                callbackHandler(response, callback);
                dispatch(switchFetchingState(false));
                dispatch(fetchLists());
            }
        );
    }
}

export function fetchLists() {
    return (dispatch, getState) => {
        dispatch(requestLists());
        const url = getSliceListUrl(getState());
        return fetch(url, {
            credentials: 'include',
            method: 'GET'
        }).then(always).then(json).then(
            response => {
                handler(response, dispatch);
                dispatch(clearRows());
            }
        );
    };
}

export function fetchSliceDelete(sliceId, callback) {
    const url = baseURL + "delete/" + sliceId;
    return dispatch => {
        return fetch(url, {
            credentials: 'include',
            method: 'GET'
        }).then(always).then(json).then(
            response => {
                callbackHandler(response, callback);
                if (response.status === 200) {
                    dispatch(fetchLists());
                }
            }
        );
    }
}

export function fetchSliceDelInfo(sliceId, callback) {
    const url = baseURL + "delete_info/" + sliceId;
    return dispatch => {
        dispatch(switchFetchingState(true));
        return fetch(url, {
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

export function fetchSliceDeleteMul(callback) {
    const url = baseURL + "muldelete/";
    return (dispatch, getState) => {
        const selectedRowKeys = getState().conditions.selectedRowKeys;
        let data = {
            selectedRowKeys: selectedRowKeys
        };
        return fetch(url, {
            credentials: 'include',
            method: 'POST',
            body: JSON.stringify(data)
        }).then(always).then(json).then(
            response => {
                callbackHandler(response, callback);
                if (response.status === 200) {
                    dispatch(fetchLists());
                }
            }
        );
    }
}

export function fetchSliceDelMulInfo(callback) {
    const url = baseURL + "muldelete_info/";
    return (dispatch, getState) => {
        dispatch(switchFetchingState(true));
        const selectedRowKeys = getState().conditions.selectedRowKeys;
        let data = {
            selectedRowKeys: selectedRowKeys
        };
        return fetch(url, {
            credentials: 'include',
            method: 'POST',
            body: JSON.stringify(data)
        }).then(always).then(json).then(
            response => {
                callbackHandler(response, callback);
                dispatch(switchFetchingState(false));
            }
        );
    }
}

export function fetchSliceDetail(sliceId, callback) {
    const url = baseURL + "show/" + sliceId;
    return dispatch => {
        return fetch(url, {
            credentials: "include",
            method: 'GET'
        }).then(always).then(json).then(
            response => {
                callbackHandler(response, callback);
            }
        );
    }
}

export function fetchUpdateSlice(state, slice, callback) {
    const url = baseURL + "edit/" + slice.id + "/";
    const newSlice = getNewSlice(slice, state.selectedDashboardObjs, state.availableDashboards);
    return dispatch => {
        return fetch(url, {
            credentials: "include",
            method: "POST",
            body: JSON.stringify(newSlice)
        }).then(always).then(json).then(
            response => {
                callbackHandler(response, callback);
                dispatch(switchFetchingState(false));
                if (response.status === 200) {
                    dispatch(fetchLists());
                }
            }
        );
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
        type: LISTS.SWITCH_FETCHING_STATE,
        isFetching: isFetching
    }
}

export function fetchDashboardList(callback) {
    const url = window.location.origin + '/dashboard/listdata/?page_size=1000';
    return fetch(url, {
        credentials: "include",
        method: 'GET'
    }).then(always).then(json).then(
        response => {
            callbackHandler(response, callback);
        }
    );
}

function getSliceListUrl(state) {
    let url = baseURL + "listdata/?page=" + (state.conditions.pageNumber - 1) +
    "&page_size=" + state.conditions.pageSize + "&filter=" + state.conditions.keyword;
    if (state.conditions.type === SHOW_FAVORITE) {
        url += "&only_favorite=1";
    }
    return url;
}

function getStateChangeUrl(record, type) {
    if (type === "favorite") {
        let url_favorite = window.location.origin + PILOT_PREFIX + "favstar/Slice/" + record.id;
        if (record.favorite) {
            url_favorite += "/unselect";
        } else {
            url_favorite += "/select";
        }
        return url_favorite;
    } else if (type === "publish") {
        let url_publish = window.location.origin + PILOT_PREFIX + "release/slice/";
        if (record.online) {
            url_publish += "offline/" + record.id;
        } else {
            url_publish += "online/" + record.id;
        }
        return url_publish;
    }
}

function getNewSlice(slice, selectedDashboardObjs, availableDashboards) {
    let obj = {};
    obj.id = slice.id;
    obj.slice_name = slice.slice_name;
    obj.description = slice.description;
    obj.dashboards = selectedDashboardObjs; //getSelectedSlices(selectedDashboards, availableDashboards);
    return obj;
}

// deprecated, used by up function
function getSelectedSlices(selectedDashboards, availableDashboards) {
    let array = [];
    selectedDashboards.forEach(function(selected) {
        availableDashboards.forEach(function(dashboard) {
            if (selected === dashboard.name) {
                array.push(dashboard);
            }
        });
    });
    return array;
}