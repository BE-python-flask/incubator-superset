import React, {Component} from 'react';
import PropTypes from 'prop-types';
import {render} from 'react-dom';
import ReactDOM from 'react-dom';
import {connect} from 'react-redux';
import {bindActionCreators} from 'redux';
import {Link, withRouter} from 'react-router-dom';
import {Select, Tooltip, TreeSelect, message} from 'antd';
import {Confirm, CreateHDFSConnect, CreateInceptorConnect} from '../../popup';
import {fetchSchemaList, datasetTypes} from '../../actions';
import {constructInceptorDataset, initDatasetData, extractOpeType, getDatasetId, extractDatasetType} from '../../module';
import {appendTreeData, constructTreeData} from '../../../../utils/common2';
import {renderLoadingModal, renderAlertTip, renderGlobalErrorMsg, fetchDatabaseList,PILOT_PREFIX,loadIntlResources} from '../../../../utils/utils';
import intl from 'react-intl-universal';

const $ = window.$ = require('jquery');

class InceptorDetail extends Component {

    constructor (props) {
        super(props);
        this.state = {
            dsInceptor: props.dsInceptor,
            disabled: 'disabled',
            initDone: false
        };
        //bindings
        this.onSave = this.onSave.bind(this);
        this.onSelectTable = this.onSelectTable.bind(this);
        this.onLoadData = this.onLoadData.bind(this);
        this.onConnectChange = this.onConnectChange.bind(this);
        this.handleChange = this.handleChange.bind(this);
        this.callbackRefresh = this.callbackRefresh.bind(this);
        this.checkIfSubmit = this.checkIfSubmit.bind(this);
        this.createInceptorConnect = this.createInceptorConnect.bind(this);
    }

    handleChange(e) {
        const target = e.currentTarget;
        const name = target.name;
        const val = target.value;
        const objInceptor = {
            ...this.state.dsInceptor,
            [name]: val
        };
        this.setState({
            dsInceptor: objInceptor
        });
    }

    onConnectChange(dbId, node) {
        const self = this;
        this.props.fetchSchemaList(dbId, callback);
        function callback(success, data) {
            if(success) {
                let treeData = constructTreeData(data, false, 'folder');
                let objInceptor = {...self.state.dsInceptor};
                objInceptor.database_id = dbId;
                objInceptor.db_name = node.props.children;
                objInceptor.treeData = treeData;
                self.setState({
                    dsInceptor: objInceptor
                });
            }else {
                renderGlobalErrorMsg(data);
            }
        }
    }

    onSelectTable(value, node) {
        if(node.props.isLeaf) {
            let dsInceptor = {
                ...this.state.dsInceptor,
                table_name: value,
            };
            this.setState({
                dsInceptor: dsInceptor
            });
            this.props.saveInceptorPreviewData({});
        }
    }

    createInceptorConnect() {
        const { dispatch } = this.props;
        render(
            <CreateInceptorConnect
                dispatch={dispatch}
                callbackRefresh={this.callbackRefresh}
            />,
            document.getElementById('popup_root')
        );
    }

    callbackRefresh() {
        this.doFetchDatabaseList();
    }

    onLoadData(node) {
        const self = this;
        const schema = node.props.value;
        const { fetchTableList } = self.props;
        return fetchTableList(self.state.dsInceptor.database_id, schema, callback);

        function callback(success, data) {
            if(success) {
                let treeData = appendTreeData(
                    schema,
                    data,
                    JSON.parse(JSON.stringify(self.state.dsInceptor.treeData))
                );
                let dsInceptor = {...self.state.dsInceptor};
                dsInceptor.schema = schema;
                dsInceptor.treeData = treeData;
                self.setState({
                    dsInceptor: dsInceptor
                });
            }else {
                renderGlobalErrorMsg(data);
            }
        }
    }

    onSave() {
        const {datasetType, saveInceptorDataset, history} = this.props;
        const opeType = extractOpeType(window.location.hash);
        const dsInceptor = constructInceptorDataset(this.state.dsInceptor);
        saveInceptorDataset(this.state.dsInceptor);
        if(window.location.hash.indexOf('/edit') > 0) {
            this.onEditInceptor(dsInceptor, opeType, datasetType, history);
        }else {
            this.onCreateInceptor(dsInceptor, opeType, datasetType, history);
        }
    }

    onEditInceptor(inceptor, opeType, datasetType, history) {
        const datasetId = getDatasetId(opeType, window.location.hash);
        this.props.editDataset(inceptor, datasetId, callback);
        function callback(success, data) {
            let response = {};
            if(success) {
                const url = '/' + opeType + '/preview/' + datasetType + '/' + datasetId;
                history.push(url);
            }else {
                response.type = 'error';
                response.message = data;
                renderAlertTip(response, 'showAlertDetail', 600);
            }
        }
    }

    onCreateInceptor(inceptor, opeType, datasetType, history) {
        const {createDataset, saveDatasetId} = this.props;
        createDataset(inceptor, callback);
        function callback(success, data) {
            let response = {};
            if(success) {
                saveDatasetId(data.object_id);
                const url = '/' + opeType + '/preview/' + datasetType + '/';
                history.push(url);
            }else {
                response.type = 'error';
                response.message = data;
                renderAlertTip(response, 'showAlertDetail', 600);
            }
        }
    }

    checkIfSubmit() {
        var fields = $(".inceptor-detail input[required]");
        let disabled = null;

        fields.each((idx, obj) => {
            if (obj.value === '') {
                disabled = 'disabled';
                return;
            }
        });
        if(disabled === this.state.disabled) {
            return;
        }
        this.setState({
            disabled: disabled
        });
    }

    componentDidMount() {
        const datasetType = extractDatasetType(window.location.hash);
        if(datasetType === datasetTypes.database) {
            this.doFetchDatabaseList();
            if(window.location.hash.indexOf('/edit') > 0) {
                this.doDatasetEdit();
            }
        }
        loadIntlResources(_ => this.setState({ initDone: true }), 'dataset');
    }

    doFetchDatabaseList() {
        const self = this;
        fetchDatabaseList(callback);
        function callback(success, data) {
            if(success) {
                let objInceptor = {
                    ...self.state.dsInceptor,
                    databases: data.data
                };
                self.setState({
                    dsInceptor: objInceptor
                });
            }else {
                renderGlobalErrorMsg(data);
            }
        }
    }

    doDatasetEdit() {
        const self = this;
        const {fetchDatasetDetail, fetchDBDetail, fetchSchemaList} = self.props;
        const datasetId = getDatasetId("edit", window.location.hash);
        fetchDatasetDetail(datasetId, callback);
        function callback(success, data) {
            if(success) {
                fetchDBDetail(data.database_id, callbackDBName);
                fetchSchemaList(data.database_id, callbackSchemaList);
                function callbackDBName(success, data_db) {
                    if(success) {
                        let objIncpetor = {
                            ...self.state.dsInceptor,
                            db_name: data_db.database_name
                        };
                        self.setState({
                            dsInceptor: initDatasetData(datasetTypes.database, data, objIncpetor)
                        });
                    }else {
                        renderGlobalErrorMsg(data_db);
                    }
                }
                function callbackSchemaList(success, data_schema) {
                    if(success) {
                        let treeData = constructTreeData(data_schema, false, 'folder');
                        let objIncpetor = {
                            ...self.state.dsInceptor,
                            treeData: treeData
                        };
                        self.setState({
                            dsInceptor: objIncpetor
                        });
                    }else {
                        renderGlobalErrorMsg(data_schema);
                    }
                }
            }
        }
    }

    componentDidUpdate() {
        this.checkIfSubmit();
    }

    componentWillReceiveProps(nextProps) {
        if(nextProps.dsInceptor.dataset_name &&
            nextProps.dsInceptor.dataset_name !== this.props.dsInceptor.dataset_name) {
            this.setState({
                dsInceptor: nextProps.dsInceptor
            });
        }
        loadIntlResources(_ => this.setState({ initDone: true }), 'dataset');
    };

    render () {
        const dsInceptor = this.state.dsInceptor;
        const Option = Select.Option;
        let dbOptions=[];
        if(this.state.dsInceptor.databases) {
            dbOptions = this.state.dsInceptor.databases.map(database => {
                return <Option key={database.id}>{database.database_name}</Option>
            });
        }
        return (this.state.initDone &&
            <div className="inceptor-detail">
                <div className="data-detail-item">
                    <div>
                        <i>*</i>
                        <span>{intl.get('DATASET.DATASET_NAME')}：</span>
                    </div>
                    <input
                        type="text"
                        name="dataset_name"
                        className="tp-input"
                        value={dsInceptor.dataset_name}
                        required="required"
                        onChange={this.handleChange}
                    />
                </div>
                <div className="data-detail-item">
                    <span>{intl.get('DATASET.DESCRIPTION')}：</span>
                    <textarea
                        name="description"
                        className="tp-textarea"
                        value={dsInceptor.description||''}
                        required="required"
                        onChange={this.handleChange}
                    />
                </div>
                <div className="data-detail-item">
                    <div>
                        <i>*</i>
                        <span>{intl.get('DATASET.SELECT_CONN')}：</span>
                    </div>
                    <Select
                        style={{ width: 230 }}
                        value={dsInceptor.db_name}
                        onSelect={this.onConnectChange}
                    >
                        {dbOptions}
                    </Select>
                    {/*<div className="connect-success">
                        <button onClick={this.createInceptorConnect}>新建连接</button>
                    </div>*/}
                    <input
                        type="hidden"
                        required="required"
                        value={dsInceptor.db_name}
                    />
                </div>
                <div className="data-detail-item">
                    <div>
                        <i>*</i>
                        <span>{intl.get('DATASET.SELECT_TABLE')}：</span>
                    </div>
                    <TreeSelect
                        showSearch
                        value={dsInceptor.table_name}
                        style={{ width: 782 }}
                        placeholder="Please select"
                        treeData={dsInceptor.treeData}
                        loadData={this.onLoadData}
                        onSelect={this.onSelectTable}
                        dropdownStyle={{ maxHeight: 400, overflow: 'auto' }}
                    >
                    </TreeSelect>
                    <input
                        type="hidden"
                        required="required"
                        value={dsInceptor.table_name}
                    />
                </div>
                <div className="data-detail-item">
                    <span>SQL：</span>
                    <textarea
                        className="tp-textarea"
                        cols="30"
                        rows="10"
                        value={dsInceptor.sql||''}
                        name="sql"
                        onChange={this.handleChange}
                    />
                    <Tooltip title={intl.get('DATASET.ADD_SQL_TIP')}>
                        <i
                            className="icon icon-info"
                            style={{right: '-5px', position: 'relative'}}
                        />
                    </Tooltip>
                    <a href={ window.location.origin + PILOT_PREFIX + 'sqllab' } target="_blank">
                        {intl.get('DATASET.SWITCH_SQL_LAB_EDIT')}
                    </a>
                </div>
                <div className="sub-btn">
                    <button onClick={this.onSave} disabled={this.state.disabled}>
                        {intl.get('DATASET.SAVE')}
                    </button>
                </div>
            </div>
        );
    }
}

export default InceptorDetail;