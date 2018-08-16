import React, { Component } from 'react';
import { connect } from 'react-redux';
import { bindActionCreators } from 'redux';
import { fetchIfNeeded, invalidateCondition, saveDatasetId, clearDatasetData } from '../actions';
import { Pagination, Table, Operate } from '../components';
import PropTypes from 'prop-types';
import intl from 'react-intl-universal';
import { renderAlertTip, loadIntlResources } from '../../../utils/utils';

class App extends Component {
    constructor(props) {
        super(props);
        this.state = {
            initDone: false
        };
    }

    componentDidMount() {
        const { dispatch, condition } = this.props;
        dispatch(fetchIfNeeded(condition));

        loadIntlResources(_ => this.setState({ initDone: true }), 'dataset');
    }

    componentWillReceiveProps(nextProps) {
        const { dispatch, condition } = nextProps;

        if (nextProps.condition.filter !== this.props.condition.filter ||
            nextProps.condition.pageSize !== this.props.condition.pageSize ||
            nextProps.condition.tableType !== this.props.condition.tableType ||
            nextProps.condition.page !== this.props.condition.page)
        {
            dispatch(invalidateCondition(condition));
            dispatch(fetchIfNeeded(condition));
        }
    }

    render() {
        const {dispatch, response, condition} = this.props;
        const count = response.count;

        return (this.state.initDone &&
            <div className="pilot-panel datasource-panel">
                <div className="panel-top">
                    <div className="left">
                        <i
                            className="icon icon-table"
                            style={{zoom: 0.8}}
                        />
                        <span>{intl.get('DATASET.DATASET')}</span>
                        <span>{intl.get('DATASET.RECORD')}</span>
                        <span>{count +''}</span>
                    </div>
                    <div className="right">
                        <Operate
                            dispatch={dispatch}
                            tableType={condition.tableType}
                            selectedRowKeys={condition.selectedRowKeys}
                            selectedRowNames={condition.selectedRowNames}
                            saveDatasetId={saveDatasetId}
                            clearDatasetData={clearDatasetData}
                        />
                    </div>
                </div>
                <div className="panel-middle">
                    <Table
                        dispatch={dispatch}
                        {...response}
                        selectedRowKeys={condition.selectedRowKeys}
                    />
                </div>
                <div className="panel-bottom">
                    <Pagination
                        dispatch={dispatch}
                        count={count}
                        pageSize={condition.pageSize}
                        pageNumber={condition.page}
                    />
                </div>
            </div>
        );
    }
}

App.propTypes = {};

function mapStateToProps(state) {
    const { condition, requestByCondition } = state;

    const {
        isFetching,
        response
    } = requestByCondition[condition.tableType]||{
        isFetching: false,
        response: {}
    }
    return {
        condition,
        response,
        isFetching
    };
}

function mapDispatchToProps(dispatch) {
  return {
    actions: bindActionCreators(actions, dispatch),
  };
}

export default connect(
    mapStateToProps
)(App);

