import { combineReducers } from 'redux';
import { actionTypes, popupActions, popupNormalActions } from '../actions';

function condition(state = {
        page_num: 1,
        page_size: 10,
        filter: '',
        path: '/user',
        paths: [],

        selectedRows: [],
        selectedRowKeys: [],
        selectedRowNames: [],

        manipulate: '',
        upload: ''
    }, action) {

    switch (action.type) {
    case actionTypes.receiveData:
        return {
            ...state,
            selectedRows: [],
            selectedRowKeys: [],
            selectedRowNames: []
        };
        break;
    case actionTypes.changePath:
        return {
            ...state,
            ...action.path
        };
        break;
    case actionTypes.search:
        return {
            ...state,
            filter: action.filter
        };
        break;
    case actionTypes.setSelectedRows:
        return {
            ...state,
            selectedRows: action.selectedRows,
            selectedRowKeys: action.selectedRowKeys,
            selectedRowNames: action.selectedRowNames
        };
        break;
    case actionTypes.navigateTo:
        return {
            ...state,
            page_num: action.pageNum
        };
        break;
    case actionTypes.changePageSize:
        return {
            ...state,
            page_size: action.pageSize
        };
        break;
    default:
        return state;
    }
}

//used only in operation bar
function popupNormalParam(state = {
        //popup callbacks
        submit: argu => argu,
        closeDialog: argu => argu,

        status: 'none', //flex, none
        disabled: 'disabled',
        showAlert: false,
        popupType: '',

        path: '',
        dir_name: '',
        dest_path: '',
        file_name: '',

        treeData: [],
        treeVal: '',

        alertStatus: 'none',
        alertMsg: '',
        alertType: '',

        permData: [],
        permMode: '',
        recursivePerm: false,

        deleteTips: ''
    }, action) {
    switch (action.type) {
    case popupNormalActions.popupChangeStatus:
        return {
            ...state,
            status: action.status
        };
        break;
    case popupNormalActions.setPopupParams:
        delete action.type;
        return {
            ...state,
            ...action
        };
        break;
    case popupNormalActions.setPopupParam:
        const param = action.param;
        const key = Object.getOwnPropertyNames(param)[0];
        return {
            ...state,
            [key]: param[key]
        };
        break;
    case popupNormalActions.receiveLeafData:
        return {
            ...state,
            treeData: action.treeData
        };
        break;
    case popupNormalActions.setPermData:
        return {
            ...state,
            permData: action.permData
        };
        break;
    case popupNormalActions.setPermMode:
        return {
            ...state,
            permMode: action.permMode
        };
        break;
    case popupNormalActions.setHDFSPath:
        return {
            ...state,
            path: action.path
        };
        break;
    case popupNormalActions.setRecursivePerm:
        return {
            ...state,
            recursivePerm: action.recursivePerm
        };
        break;
    case popupNormalActions.setTreeData:
        return {
            ...state,
            treeData: action.treeData
        };
        break;
    default:
        return state;
    }
}

function emitFetch(state = {
        isFetching: false,
        didInvalidate: false,
        response: [],
        heap: []
    }, action) {
    switch (action.type) {
    case actionTypes.invalidateCondition:
        return {
            ...state,
            didInvalidate: true
        };
        break;
    case actionTypes.receiveData:
        return {
            ...state,
            response: action.response,
            heap: action.response
        };
        break;
    case actionTypes.switchFetchingStatus:
        return {
            ...state,
            isFetching: action.isFetching
        };
        break;
    case popupNormalActions.swapResponse:
        return {
            ...state,
            response: action.response
        }
        break;
    default:
        return state;
        break
    }
}

function fileReducer(state = {
        path: '',
        mtime: '', //last time modify
        size: '',
        user: '',
        group: '',
        mode: '',

        preview: ''
    }, action) {
    switch (action.type) {
    case actionTypes.giveDetail:
        return {
            ...state,
            ...action.detail
        };
        break;

    case actionTypes.setPreview:
        return {
            ...state,
            preview: action.preview
        };
        break;
    default:
        return state;
    }
}

const rootReducer = combineReducers({
    condition,
    // popupParam,
    popupNormalParam,
    emitFetch,
    fileReducer
});

export default rootReducer;
