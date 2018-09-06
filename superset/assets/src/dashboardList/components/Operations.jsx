import React from 'react';
import {render} from 'react-dom';
import {Provider, connect} from 'react-redux';
import PropTypes from 'prop-types';
import {Link } from 'react-router-dom';
import {DashboardAdd, DashboardDelete, ImportDashboard} from '../popup';
import * as actions from '../actions';
import intl from 'react-intl-universal';
import {renderGlobalErrorMsg} from '../../utils/utils';

class Operations extends React.Component {
    constructor(props) {
        super(props);
        this.state = {};
        // bindings
        this.addDashboard = this.addDashboard.bind(this);
        this.deleteDashboardMul = this.deleteDashboardMul.bind(this);
        this.importDashboard = this.importDashboard.bind(this);
        this.exportDashboard = this.exportDashboard.bind(this);
        this.showAll = this.showAll.bind(this);
        this.showFavorite = this.showFavorite.bind(this);
        this.searchDashboard = this.searchDashboard.bind(this);
        this.clickSearchDashboard = this.clickSearchDashboard.bind(this);
        this.keywordChange = this.keywordChange.bind(this);
        this.switchTableMode = this.switchTableMode.bind(this);
        this.switchGraphMode = this.switchGraphMode.bind(this);
    };

    addDashboard() {

        const { dispatch } = this.props;
        const callback = (success, data) => {
            if(success) {
                const dashboard = {name: '', description: ''};
                render(
                    <DashboardAdd
                        dispatch={dispatch}
                        dashboard={dashboard}
                        availableSlices={data.data}
                        enableConfirm={false}/>,
                    document.getElementById('popup_root')
                );
            }
        }
        actions.fetchAvailableSlices(callback);
    }

    deleteDashboardMul() {
        const { dispatch, selectedRowNames } = this.props;
        const callback = function(success, data) {
            if(success) {
                let deleteType = "multiple";
                let deleteTips = data.length===0 ? intl.get('DASHBOARD.CONFIRM_TO_DELETE') + selectedRowNames + '?' : data;
                if(selectedRowNames.length === 0) {
                    deleteType = 'none';
                    deleteTips = intl.get('DASHBOARD.DELETE_TIP');
                }
                render(
                    <DashboardDelete
                        dispatch={dispatch}
                        deleteType={deleteType}
                        deleteTips={deleteTips}
                    />,
                    document.getElementById('popup_root')
                );
            }else {
                renderGlobalErrorMsg(data);
            }
        }
        dispatch(actions.fetchDashboardMulDelInfo(callback));
    }

    importDashboard() {
        const { dispatch } = this.props;
        const callback = (success, data) => {
            if(success) {
                render(
                    <ImportDashboard dispatch={dispatch} />,
                    document.getElementById('popup_root')
                );
            }
        }
        actions.fetchAvailableSlices(callback);
    }

    exportDashboard() {
        this.props.dispatch(actions.fetchDashboardExport(_ => {
            console.log('exportDashboard succeed');
            //TODO: notification 
        }));
    }

    switchTableMode() {
        const {dispatch} = this.props;
        dispatch(actions.setViewMode('table'));
    }

    switchGraphMode() {
        const {dispatch} = this.props;
        dispatch(actions.setViewMode('graph'));
    }

    searchDashboard(event) {
        if(event.keyCode === 13) {
            const { dispatch } = this.props;
            dispatch(actions.fetchPosts());
            dispatch(actions.setPageNumber(1));
        }
    }

    clickSearchDashboard() {
        const {dispatch} = this.props;
        dispatch(actions.fetchPosts());
        dispatch(actions.setPageNumber(1));
    }

    keywordChange(event) {
        const { dispatch } = this.props;
        dispatch(actions.setKeyword(event.target.value));
        if(event.target.value === "") {
            dispatch(actions.fetchPosts());
            dispatch(actions.setPageNumber(1));
        }
    }

    showAll() {
        const { dispatch } = this.props;
        dispatch(actions.setPageNumber(1));
        dispatch(actions.setShowType('show_all'));
        dispatch(actions.setKeyword(''));
        dispatch(actions.fetchPosts());
    }

    showFavorite() {
        const { dispatch } = this.props;
        dispatch(actions.setPageNumber(1));
        dispatch(actions.setShowType('show_favorite'));
        dispatch(actions.setKeyword(''));
        dispatch(actions.fetchPosts());
    }

    render() {
        const { typeName, viewMode } = this.props;
        return (
            <div className="operations">
                <ul className="icon-list">
                    <li onClick={this.addDashboard}><i className="icon icon-plus"/></li>
                    <li onClick={this.deleteDashboardMul}><i className="icon icon-trash"/></li>
                    <li onClick={this.importDashboard}><i className="icon icon-import"/></li>
                    <li onClick={this.exportDashboard}><i className="icon icon-export"/></li>
                </ul>
                <div className="tab-btn">
                    <button className={typeName === 'show_all' ? 'active' : ''} onClick={this.showAll}>
                        {intl.get('DASHBOARD.ALL')}
                    </button>
                    <button className={typeName === 'show_favorite' ? 'active' : ''} onClick={this.showFavorite}>
                        <i className={typeName === 'show_favorite' ? 'icon icon-star-active' : 'icon icon-star'}/>
                        {intl.get('DASHBOARD.FAVORITE')}
                    </button>
                </div>
                <div className="search-input">
                    <input
                        onKeyUp={this.searchDashboard} onChange={this.keywordChange}
                        className="tp-input" placeholder={intl.get('DASHBOARD.SEARCH')}
                    />
                    <i className="icon icon-search" onClick={this.clickSearchDashboard}/>
                </div>
            </div>
        );
    }
}

const propTypes = {};
const defaultProps = {};

Operations.propTypes = propTypes;
Operations.defaultProps = defaultProps;

export default Operations;