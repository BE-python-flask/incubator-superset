import { combineReducers } from 'redux';
import {
    CHANGE_CATAGORY_IN_TENDENCY,
    SWITCH_TAB_IN_FAVOURITE,
    SWITCH_TAB_IN_EDIT,
    VisibilityCatagory,
    REQUEST_POSTS,
    RECEIVE_POSTS
} from "../actions";

function switcher(state = {
    tendencyCatagory: 'dashboard',
    editPanelCatagory: 'dashboard',
    favoritePanelCatagory: 'dashboard'
}, action) {
    switch (action.type) {
        case CHANGE_CATAGORY_IN_TENDENCY:
            return Object.assign({}, state, {
                tendencyCatagory: action.catagory
            });
        case SWITCH_TAB_IN_FAVOURITE:
            return Object.assign({}, state, {
                favoritePanelCatagory: action.tab
            });
        case SWITCH_TAB_IN_EDIT:
            return Object.assign({}, state, {
                editPanelCatagory: action.tab
            });
        default:
            return state;
    }
}

function posts(state = {
    isFetching: true,
    param: {}
}, action) {
    switch (action.type) {
        case REQUEST_POSTS:
            return Object.assign({}, state, {
                isFetching: true,
            });
        case RECEIVE_POSTS:
            return Object.assign({}, state, {
                isFetching: false,
                param: action.posts,
                lastUpdated: action.receivedAt
            });
        default:
            return state;
    }
}

const rootReducer = combineReducers({
    switcher,
    posts
});

export default rootReducer;