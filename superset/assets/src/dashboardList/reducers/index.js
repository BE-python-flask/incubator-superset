/**
 * Created by haitao on 17-5-18.
 */
import { combineReducers } from 'redux';
import { CONFIG_PARAMS, setBinary, setImportParams } from '../actions';

function configs(state = {
        type: "show_all",
        keyword: "",
        pageNumber: 1,
        pageSize: 10,
        selectedRowKeys: [],
        selectedRowNames: [],
        viewMode: 'table',
        tableLoading: false
    }, action) {
    switch (action.type) {
    case CONFIG_PARAMS.SHOW_TYPE:
        return Object.assign({}, state, {
            type: action.typeName
        });
        break;
    case CONFIG_PARAMS.PAGE_NUMBER:
        return Object.assign({}, state, {
            pageNumber: action.pageNumber
        });
        break;
    case CONFIG_PARAMS.PAGE_SIZE:
        return Object.assign({}, state, {
            pageSize: action.pageSize
        });
        break;
    case CONFIG_PARAMS.SET_KEYWORD:
        return Object.assign({}, state, {
            keyword: action.keyword
        });
        break;
    case CONFIG_PARAMS.SELECTED_ROWS:
        return Object.assign({}, state, {
            selectedRowKeys: action.selectedRowKeys,
            selectedRowNames: action.selectedRowNames
        });
    case CONFIG_PARAMS.CLEAR_ROWS:
        return Object.assign({}, state, {
            selectedRowKeys: action.selectedRowKeys,
            selectedRowNames: action.selectedRowNames
        });
    case CONFIG_PARAMS.VIEW_MODE:
        return Object.assign({}, state, {
            viewMode: action.viewMode
        });
    case CONFIG_PARAMS.TABLE_LOADING:
        return Object.assign({}, state, {
            tableLoading: action.tableLoading
        });
    default:
        return state;
    }
}

function posts(state = {
        isFetching: false,
        params: {}
    }, action) {
    switch (action.type) {
    case "REQUEST_POSTS":
        return Object.assign({}, state, {
            isFetching: true
        });
    case "RECEIVE_POSTS":
        return Object.assign({}, state, {
            isFetching: false,
            params: action.posts
        });
    case "SWITCH_FETCHING_STATE":
        return Object.assign({}, state, {
            isFetching: action.isFetching
        });
    default:
        return state;
    }
}

function details(state = {
        isFetching: true,
        dashboardDetail: {}
    }, action) {
    switch (action.type) {
    case "REQUEST_DASHBOARD_DETAIL":
        return Object.assign({}, state, {
            isFetching: true
        });
        break;
    case "RECEIVE_DASHBOARD_DETAIL":
        return Object.assign({}, state, {
            isFetching: false,
            dashboardDetail: action.detail
        });
        break;
    default:
        return state;
    }
}


function importParams(state = {
        binaryFile: '',
        paramData: {}
    }, action) {
    switch (action.type) {
    case setBinary:
        return Object.assign({}, state, {
            binaryFile: action.binaryFile
        });
    case setImportParams:
        return Object.assign({}, state, {
            paramData: action.paramData
        });
    default:
        return state;
    }
}

const rootReducer = combineReducers({
    configs,
    posts,
    details,
    importParams
});

export default rootReducer;