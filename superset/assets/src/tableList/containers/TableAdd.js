import React, { Component } from 'react';
import * as module from '../module';
import { HashRouter, Route, NavLink, Switch } from 'react-router-dom';
import { connect, Provider } from 'react-redux';
import { SubDetail, SubPreview, SubColumns, SubSqlMetric } from './';
import '../style/tableAdd';
import { loadIntlResources } from '../../../utils/utils';
import intl from 'react-intl-universal';

class TableAdd extends Component {
    constructor(props) {
        super(props);
        this.state = {};
    }

    componentDidMount() {
        console.log('componentDidMount...');
        loadIntlResources(_ => this.setState({
            initDone: true
        }), 'dataset');
    }

    render() {
        const {datasetId, HDFSConfigured} = this.props;
        const path = window.location.hash;
        const datasetType = module.extractDatasetType(path);
        const tab2Name = module.getDatasetTab2Name(datasetType, intl);
        const opeType = module.extractOpeType(path);
        const title = module.getDatasetTitle(opeType, datasetType, intl);
        const id = module.getDatasetId(opeType, path);
        const enableClick = module.judgeEnableClick(opeType, datasetId);
        const enableClickHDFSPreview = module.judgeEnableClickHDFSPreview(
            opeType, datasetType, datasetId, HDFSConfigured);
        const isDetailActive = (match, location) => {
            return module.isActive('detail', location);
        };
        const isPreviewActive = (match, location) => {
            return module.isActive('preview', location);
        };
        const isColumnsActive = (match, location) => {
            return module.isActive('columns', location);
        };
        const isSqlMetricActive = (match, location) => {
            return module.isActive('sqlMetric', location);
        };
        return (
            <div className="data-detail">
                <h1 className="title">
                    <i className="icon"/>{title}
                </h1>
                <div className="data-detail-wrap">
                    <div className="data-wrap-center">
                        <nav className="detail-nav">
                            <ul>
                                <li>
                                    <NavLink to={`/${opeType}/detail/${datasetType}/${id}`} activeClassName="active" isActive={isDetailActive}>
                                        {intl.get('DATASET.BASIC_INFO')}
                                    </NavLink>
                                </li>
                                <li>
                                    <NavLink to={`/${opeType}/preview/${datasetType}/${id}`} activeClassName="active" className={enableClickHDFSPreview ? '' : 'link-disabled'} isActive={isPreviewActive} >
                                        { tab2Name }
                                    </NavLink>
                                </li>
                                <li>
                                    <NavLink to={`/${opeType}/columns/${datasetType}/${id}`} activeClassName="active" className={enableClick ? '' : 'link-disabled'} isActive={isColumnsActive}>
                                        {intl.get('DATASET.COLUMN_ATTRIBUTE')}
                                    </NavLink>
                                </li>
                                <li>
                                    <NavLink to={`/${opeType}/sqlMetric/${datasetType}/${id}`} activeClassName="active" className={enableClick ? '' : 'link-disabled'} isActive={isSqlMetricActive}>
                                        {intl.get('DATASET.METRIC')}
                                    </NavLink>
                                </li>
                            </ul>
                        </nav>
                    </div>
                </div>
                <div>
                    <Switch>
                        <Route exact path={`/${opeType}/detail/:type`} component={SubDetail}/>
                        <Route path={`/${opeType}/detail/:type/:id`} component={SubDetail}/>
                        <Route exact path={`/${opeType}/preview/:type`} component={SubPreview}/>
                        <Route path={`/${opeType}/preview/:type/:id`} component={SubPreview}/>
                        <Route exact path={`/${opeType}/columns/:type`} component={SubColumns}/>
                        <Route path={`/${opeType}/columns/:type/:id`} component={SubColumns}/>
                        <Route exact path={`/${opeType}/sqlMetric/:type`} component={SubSqlMetric}/>
                        <Route path={`/${opeType}/sqlMetric/:type/:id`} component={SubSqlMetric}/>
                    </Switch>
                </div>
            </div>
        );
    }
}

function mapStateToProps(state) {
    const {subDetail} = state;
    return {
        datasetId: subDetail.datasetId,
        HDFSConfigured: subDetail.HDFSConfigured
    };
}

export default connect(mapStateToProps)(TableAdd);