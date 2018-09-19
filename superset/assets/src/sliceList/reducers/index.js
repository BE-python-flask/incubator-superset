import { combineReducers } from 'redux';
import {
    LISTS, DETAILS, CONDITION_PARAMS
} from '../actions';

function conditions(state = {
    type: "showAll",
    keyword: "",
    pageNumber: 1,
    pageSize: 10,
    selectedRowKeys: [],
    selectedRowNames: [],
    tableLoading: false,

}, action) {
    switch (action.type) {
        case CONDITION_PARAMS.SHOW_TYPE:
            return Object.assign({}, state, {
                type: action.typeName
            });
            break;
        case CONDITION_PARAMS.PAGE_NUMBER:
            return Object.assign({}, state, {
                pageNumber: action.pageNumber
            });
            break;
        case CONDITION_PARAMS.PAGE_SIZE:
            return Object.assign({}, state, {
                pageSize: action.pageSize
            });
            break;
        case CONDITION_PARAMS.KEYWORD:
            return Object.assign({}, state, {
                keyword: action.keyword
            });
            break;
        case CONDITION_PARAMS.SELECTED_ROWS:
            return Object.assign({}, state, {
                selectedRowKeys: action.selectedRowKeys,
                selectedRowNames: action.selectedRowNames
            });
        case CONDITION_PARAMS.CLEAR_ROWS:
            return Object.assign({}, state, {
                selectedRowKeys: action.selectedRowKeys,
                selectedRowNames: action.selectedRowNames
            });
        case CONDITION_PARAMS.TABLE_LOADING:
            return Object.assign({}, state, {
                tableLoading: action.tableLoading
            });
        default:
            return state;
    }
}

function lists(state = {
    isFetching: false,
    items: []
}, action) {
    switch (action.type) {
        case LISTS.REQUEST_LISTS:
            return Object.assign({}, state, {
                isFetching: true
            });
        case LISTS.RECEIVE_LISTS:
            return Object.assign({}, state, {
                isFetching: false,
                items: action.lists
            });
        case LISTS.SWITCH_FETCHING_STATE:
            return Object.assign({}, state, {
                isFetching: action.isFetching
            });
        default:
            return state;
    }
}

function details(state = {

}, action){
    switch (action.type) {
        default:
            return state;
    }
}

const rootReducer = combineReducers({
    conditions,
    lists,
    details
});

export default rootReducer;
