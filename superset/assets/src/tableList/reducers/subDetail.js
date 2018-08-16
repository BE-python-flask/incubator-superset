import { combineReducers } from 'redux';
import { actionTypes } from '../actions';
import { REHYDRATE } from 'redux-persist/constants';

export default function subDetail(state = {
        //all below are radios
        datasetType: '', //DATABASE, HDFS, UPLOAD FILE
        HDFSConnected: true,
        HDFSConfigured: false,
        datasetId: '',
        inceptorPreviewData: {}, //for local cache
        dsHDFS: {
            dataset_name: '',
            description: '',
            // hdfsConnectId: '',
            // hdfsConnectName: '',
            inceptorConnectName: '',
            hdfsPath: '',
            uploadFileName: '',
            fileUploaded: false,
            file_type: 'csv',
            separator: ',',
            quote: '\\',
            next_as_header: false,
            charset: 'utf-8'
        },
        dsInceptor: {
            dataset_name: '',
            description: '',
            db_name: '',
            table_name: '',
            database_id: '',
            sql: ''
        },
        isFetching: false
    }, action) {
    switch (action.type) {
    case actionTypes.switchDatasetType:
        return {
            ...state,
            datasetType: action.datasetType
        };
        break;
    case actionTypes.switchHDFSConnected:
        return {
            ...state,
            HDFSConnected: action.HDFSConnected
        };
        break;
    case actionTypes.saveDatasetId:
        return {
            ...state,
            datasetId: action.datasetId
        };
        break;
    case actionTypes.saveInceptorPreviewData:
        return {
            ...state,
            inceptorPreviewData: action.inceptorPreviewData
        };
        break;
    case actionTypes.saveInceptorDataset:
        return {
            ...state,
            dsInceptor: action.dsInceptor
        };
        break;
    case actionTypes.saveHDFSDataset:
        return {
            ...state,
            dsHDFS: action.dsHDFS,
            HDFSConfigured: action.HDFSConfigured
        };
        break;
    case actionTypes.switchFetchingState:
        return {
            ...state,
            isFetching: action.isFetching
        };
        break;
    case REHYDRATE:
        return {
            ...state, ...action.payload.subDetail
        };
        break;
    case actionTypes.clearDatasetData:
        return {
            ...state,
            dsHDFS: {},
            dsInceptor: {},
            dsUpload: {},
            inceptorPreviewData: {}
        };
        break;
    default:
        return state;
    }
}
