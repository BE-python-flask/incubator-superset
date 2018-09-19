import React, { Component } from 'react';
import { connect } from 'react-redux';
import { fetchLists } from '../actions';
import { Pagination, Table, Operate } from '../components';
import PropTypes from 'prop-types';
import intl from "react-intl-universal";
import { renderAlertTip, loadIntlResources } from '../../utils/utils';

class App extends Component {
    constructor(props) {
        super(props);
        this.state = {
            initDone: false
        };
    }

    componentDidMount() {
        const { dispatch } = this.props;
        dispatch(fetchLists());
        loadIntlResources(_ => this.setState({ initDone: true }), 'slice');
    }

    render() {
        const {dispatch, lists, conditions} = this.props;

        return (
            this.state.initDone &&
            <div className="pilot-panel slice-panel">
                <div className="panel-top">
                    <div className="left">
                        <i className="icon icon-slice" style={{zoom: 0.9}}/>
                        <span>{intl.get('SLICE.SLICE')}</span>
                        <span>{intl.get('SLICE.RECORD')}</span>
                        <span>{lists.items.count +''}</span>
                    </div>
                    <div className="right">
                        <Operate
                            dispatch={dispatch}
                            typeName={conditions.type}
                            selectedRowKeys={conditions.selectedRowKeys}
                            selectedRowNames={conditions.selectedRowNames}
                        />
                    </div>
                </div>
                <div className="panel-middle">
                    <Table
                        dispatch={dispatch}
                        loading={conditions.tableLoading}
                        selectedRowKeys={conditions.selectedRowKeys}
                        sliceList={lists.items.data}
                    />
                </div>
                <div className="panel-bottom">
                    <Pagination
                        dispatch={dispatch}
                        count={lists.items.count}
                        pageSize={conditions.pageSize}
                        pageNumber={conditions.pageNumber}
                    />
                </div>
            </div>
        );
    }
}

App.propTypes = {};

function mapStateToProps(state) {
    return {
        lists: state.lists,
        details: state.details,
        conditions: state.conditions
    }
}

export default connect(mapStateToProps)(App);

