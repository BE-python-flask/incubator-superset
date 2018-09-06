import React from 'react';
import { render } from 'react-dom';
import { Provider, connect } from 'react-redux';
import thunk from 'redux-thunk';
import PropTypes from 'prop-types';
import { fetchPosts, setViewMode } from '../actions';
import { Operations, Tables, Paginations, Gallery } from '../components';
import intl from 'react-intl-universal';
import { renderAlertTip, loadIntlResources } from '../../utils/utils';

class GraphContainer extends React.Component {
    constructor(props) {
        super(props);
        this.state = {
            initDone: false
        };
    };

    componentDidMount() {
        const { dispatch } = this.props;
        dispatch(fetchPosts());
        dispatch(setViewMode('graph'));//for refresh browser
        loadIntlResources(_ => this.setState({ initDone: true }), 'dashboard');
    }

    render() {
        const { dispatch, posts, configs } = this.props;
        return (this.state.initDone &&
            <div className="pilot-panel dashboard-panel">
                <div className="panel-top">
                    <div className="left">
                        <i className="icon icon-dashboard"/>
                        <span>{intl.get('DASHBOARD.DASHBOARD')}</span>
                        <span>{intl.get('DASHBOARD.RECORD')}</span>
                        <span>{posts.params.count}</span>
                    </div>
                    <div className="right">
                        <Operations
                            dispatch={dispatch}
                            typeName={configs.type}
                            viewMode={configs.viewMode}
                            selectedRowKeys={configs.selectedRowKeys}
                            selectedRowNames={configs.selectedRowNames}/>
                    </div>
                </div>
                <div className="panel-middle">
                    <Gallery
                        dispatch={dispatch}
                        dashboardList={posts.params.data}
                        selectedRowKeys={configs.selectedRowKeys}
                        selectedRowNames={configs.selectedRowNames}/>
                </div>
                <div className="panel-bottom">
                    <Paginations
                        dispatch={dispatch}
                        count={posts.params.count}
                        pageSize={configs.pageSize}
                        pageNumber={configs.pageNumber}/>
                </div>
            </div>
        );
    }
}

const propTypes = {};
const defaultProps = {};

GraphContainer.propTypes = propTypes;
GraphContainer.defaultProps = defaultProps;

function addTableKey(posts) {
    if(posts.params.data) {
        posts.params.data.forEach(function(table) {
            table.key = table.id;
        });
    }
    return posts;
}

function mapStateToProps(state) {
    return {
        posts: addTableKey(state.posts),
        configs: state.configs,
        details: state.details
    }
}

export default connect(mapStateToProps)(GraphContainer);
