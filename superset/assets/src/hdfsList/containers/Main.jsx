import React, { Component } from 'react';
import { connect } from 'react-redux';
import { bindActionCreators } from 'redux';
import * as actions from '../actions';
import { Pagination, Table, Operate, PopupConnections } from '../components';
import PropTypes from 'prop-types';
import { 
    renderLoadingModal, 
    loadIntlResources, 
    getUrlParam, 
    renderGlobalErrorMsg 
} from './../../utils/utils';

import intl from "react-intl-universal";

class Main extends Component {

    getChildContext() {
        const {dispatch, condition} = this.props;
        this.dispatch = dispatch;
        return {
            dispatch: dispatch
        }

    }

    constructor(props, context) {
        super(props);
        const {condition} = this.props;

        this.state = {
            initDone: false,
            breadCrumbEditable: false,
            breadCrumbText: condition.path
        };
    }

    componentWillMount() {
        console.log('componentWillMount');
    }

    componentDidMount() {
        const {condition, changePath} = this.props;
        this.loadLocales();
        const current_path = getUrlParam('current_path').replace('#/', '');

        current_path && changePath({
            path: current_path
        });
        this.props.fetchIfNeeded(condition);

        const error_message = getUrlParam('error_message').replace('#/', '');
        error_message && renderGlobalErrorMsg(decodeURIComponent(error_message));
    }

    loadLocales() {
        loadIntlResources(_ => this.setState({ initDone: true }));
    }

    componentWillReceiveProps(nextProps) {
        const {condition, popupNormalParam} = nextProps;

        this.setState({
            breadCrumbText: condition.path
        });

        if (condition.filter !== this.props.condition.filter ||
            condition.path !== this.props.condition.path ||
            condition.page_num !== this.props.condition.page_num ||
            condition.page_size !== this.props.condition.page_size
        ) {
            this.props.fetchIfNeeded(condition);
        }
    }

    breadCrumbEditable() {
        this.setState({
            breadCrumbEditable: !this.state.breadCrumbEditable
        })
    }

    //S:Path 
    onPathChange(e) {
        let val = e.currentTarget.value.trim();

        this.setState({
            breadCrumbText: val || '/'
        })
    }

    pathAdjust(val) {
        return val.lastIndexOf('/') === val.length - 1 ?
            val.substr(0, val.length - 1) : val;
    }

    onPathBlur(e) {
        const {dispatch, condition, changePath} = this.props;
        let val = e.currentTarget.value.trim();
        this.setState({
            breadCrumbEditable: false
        });
        if (val === condition.path) {
            return;
        }

        val = this.pathAdjust(val);

        changePath({
            path: val
        });
    }

    linkToPath(ag) {
        const { changePath, fetchIfNeeded, condition, navigateTo } = this.props;
        changePath(ag);
        navigateTo(1);
        if(ag.path === condition.path) {
            fetchIfNeeded(condition);
        }
    }

    navigation(ag) {
        ag.target.dataset.href && this.linkToPath({
            path: ag.target.dataset.href
        });
    }

    //E:Path

    render() {
        const {giveDetail, condition, emitFetch, setSelectedRows, fetchIfNeeded} = this.props;
        const editable = this.state.breadCrumbEditable;
        const response = emitFetch.response;

        let count = 0,
            breadCrumbText = this.state.breadCrumbText;
        if (response && response.page) {
            count = response.page.total_count;
        }

        //path
        //related to condition;
        let translate = (path) => {

            let arr = path.split('/');
            let ar = [];
            let pathString;
            let deep = 1;
            let result = [];

            if (path === '/') {
                result.push({
                    href: '/',
                    show: '/'
                })
            } else {
                arr[arr.length - 1] === '/' && arr.pop();

                for (let i = 1; i <= arr.length; i++) {
                    ar.push(arr[i - 1]);
                    pathString = ar.join('/');
                    let param = {
                        href: pathString || '/',
                        show: arr[i - 1] || '/'
                    }
                    result.push(param);
                }
            }

            return result;
        };

        let paths = translate(breadCrumbText);
        let breadCrumbChildren;
        if (paths.length >3 ) {
            paths = paths.slice(paths.length-3);
            breadCrumbChildren = paths.map((obj, idx) => {
                console.log(obj.show, obj.href);
                return <a key={idx + 1} data-href={obj.href}>{(idx >= 1 ? '/' : '') + obj.show}</a>;
            });
        } else {
            breadCrumbChildren = paths.map((obj, idx) => {
                return <a key={idx + 1} data-href={obj.href}>{(idx >= 2 ? '/' : '') + obj.show}</a>;
            });
        }

        return (this.state.initDone && 
            <div className="pilot-panel hdfs-panel">
                <div className="panel-top">
                    <div className="bread-crumb">
                        <span className="f16">{intl.get('path')}</span>
                        <span
                            className="anchor"
                            onClick={ag => this.navigation(ag)}
                        >{breadCrumbChildren}</span>
                        <input
                            id="breadCrumbText"
                            className="editing"
                            type="text"
                            value={breadCrumbText}
                            style={{
                                display: (editable ? 'inline-block' : 'none')
                            }}
                            onBlur={e => this.onPathBlur(e)}
                            onChange={e => this.onPathChange(e)}
                            autoComplete="off" />
                        <i
                            className="icon icon-edit ps-edit-icon"
                            onClick={() => this.breadCrumbEditable()} />
                    </div>
                    <div className="right">
                        <Operate
                            tableType={condition.tableType}
                            selectedRowKeys={condition.selectedRowKeys}
                            selectedRowNames={condition.selectedRowNames}
                        />
                    </div>
                </div>
                <div className="panel-middle">
                    <Table
                        {...response}
                        giveDetail={giveDetail}
                        condition={condition}
                        linkToPath={(e) => this.linkToPath(e)}
                        setSelectedRows={setSelectedRows}
                        fetchIfNeeded={fetchIfNeeded}
                    />
                </div>
                <div className="panel-bottom">
                    <Pagination
                        count={count}
                        pageSize={condition.page_size}
                        pageNumber={condition.page_num}
                    />
                </div>
            </div>
        );
    }
}

Main.propTypes = {};

function mapStateToProps(state, pros) {
    const {condition, popupParam, emitFetch, popupNormalParam} = state;

    return {
        condition,
        emitFetch,
        popupParam,

        popupNormalParam
    };
}

function mapDispatchToProps(dispatch) {
    const {changePath, navigateTo, giveDetail, setPopupParam, popupChangeStatus, setSelectedRows, fetchIfNeeded} = bindActionCreators(actions, dispatch);

    return {
        changePath,
        navigateTo,
        giveDetail,
        setPopupParam,
        fetchIfNeeded,
        popupChangeStatus,
        setSelectedRows,
        dispatch
    };
}

Main.childContextTypes = {
    dispatch: PropTypes.func.isRequired
}

export default connect(
    mapStateToProps,
    mapDispatchToProps
)(Main);

