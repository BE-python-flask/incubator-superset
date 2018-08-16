import React, {Component} from 'react';
import PropTypes from 'prop-types';
import {connect} from 'react-redux';
import {render} from 'react-dom';
import {bindActionCreators} from 'redux';
import {Link, withRouter} from 'react-router-dom';
import * as actionCreators from '../actions';
import {datasetTypes} from '../actions';
import {Table, Select, Tooltip} from 'antd';
import * as datasetModule from '../module';
import {DetailType} from '../popup';
import intl from 'react-intl-universal';
import {renderAlertTip, renderGlobalErrorMsg, loadIntlResources} from '../../../utils/utils';

class SubPreview extends Component {

    constructor (props) {
        super(props);
        this.state = {
            tableWidth: '100%',
            tbTitle: [],
            columnNames: [],
            operatorChange: false,
            initDone: false,
            dsHDFS: this.initHDFSDataset(props)
        };
        //bindings
        this.charsetChange = this.charsetChange.bind(this);
        this.handleChange = this.handleChange.bind(this);
        this.saveHDFSDataset = this.saveHDFSDataset.bind(this);
        this.previewHDFSDataset = this.previewHDFSDataset.bind(this);
        this.onTypeSelect = this.onTypeSelect.bind(this);
        this.hdfsColumnNameChange = this.hdfsColumnNameChange.bind(this);
    }

    initHDFSDataset(props) {
        const hashUrl = window.location.hash;
        const dsHDFS = datasetModule.initHDFSPreviewData(
            props.dsHDFS,
            datasetModule.extractOpeType(hashUrl)
        );
        return dsHDFS;
    }

    componentDidMount() {
        const { datasetId, inceptorPreviewData, dsHDFS } = this.props;
        const datasetType = datasetModule.extractDatasetType(window.location.hash);
        if(datasetType === datasetTypes.database) {
            if(JSON.stringify(inceptorPreviewData) === "{}") {
                if(datasetId && datasetId.toString().length > 0) {
                    this.doFetchInceptorPreviewData(datasetId);
                }
            }else {//local cached
                this.doConstructTableData(datasetType, inceptorPreviewData, true);
            }
        } else if(datasetType === datasetTypes.hdfs
            || datasetType === datasetTypes.uploadFile) {

            //if(dsHDFS && dsHDFS.hdfsConnectId) {//local not cached
                this.doFetchHDFSPreviewData(dsHDFS, false);
            //}
        }

        loadIntlResources(_ => this.setState({ initDone: true }), 'dataset');
    }

    componentWillReceiveProps(nextProps) {
        const datasetType = datasetModule.extractDatasetType(window.location.hash);
        if(datasetType === datasetTypes.database) {
            if(nextProps.datasetId !== this.props.datasetId && nextProps.datasetId) {
                this.doFetchInceptorPreviewData(nextProps.datasetId);
            }
        }else if(datasetType === datasetTypes.hdfs
            || datasetType === datasetTypes.uploadFile) {
            if(nextProps.dsHDFS.hdfsConnectId !== this.props.dsHDFS.hdfsConnectId
                && nextProps.dsHDFS.hdfsConnectId) {
                this.doFetchHDFSPreviewData(nextProps.dsHDFS, false);
                this.setState({dsHDFS: nextProps.dsHDFS});
            }
        }
    }

    handleChange(e) {
        const target = e.currentTarget;
        const name = target.name;
        let val = target.value;
        if(name === 'next_as_header') {
            val = target.checked;
        }
        if(name === 'separator' || name === 'next_as_header') {//seperator can case columns number change Currently
            this.setState({
                operatorChange: true
            });
        }
        const objHDFS = {
            ...this.state.dsHDFS,
            [name]: val
        };
        this.setState({
            dsHDFS: objHDFS
        });
    }

    hdfsColumnNameChange(e) {
        const target = e.currentTarget;
        const val = target.value;
        const key = target.name;
        const tbTitleOnly = JSON.parse(JSON.stringify(this.state.tbTitle));
        tbTitleOnly.map((column) => {
            if(column.key === key) {
                column.title = val;
                column.dataIndex = val;
            }
        });
        const tbTitle = datasetModule.getTbTitleHDFS(
            JSON.parse(JSON.stringify(tbTitleOnly)),
            this, this.state.tbTitle, false
        );
        const columnNames = this.constructColumnNames(tbTitle);
        this.setState({
            tbTitle: tbTitle,
            columnNames: columnNames
        });
    }

    doFetchInceptorPreviewData(datasetId) {
        const self = this;
        const {fetchInceptorPreviewData, saveInceptorPreviewData} = this.props;
        fetchInceptorPreviewData(datasetId, callback);
        function callback(success, data) {
            if(success) {
                let width = datasetModule.getTableWidth(data.columns.length);
                saveInceptorPreviewData(data);
                self.setState({
                    tableWidth: width,
                    data: data
                });
                self.doConstructTableData(datasetTypes.database, data, false);
            }else {
                renderGlobalErrorMsg(data);
            }
        }
    }

    doFetchHDFSPreviewData(dsHDFS, isClickPreview) {
        const self = this;
        const {fetchHDFSPreviewData, datasetId} = this.props;
        fetchHDFSPreviewData(dsHDFS, datasetId, callback);
        function callback(success, data) {
            if(success) {
                let width = datasetModule.getTableWidth(data.columns.length);
                let columnsNeedChange = (!isClickPreview) || (isClickPreview && self.state.operatorChange);
                self.setState({
                    tableWidth: width,
                    data: data,
                    operatorChange: false
                });
                if((self.state.columnNames && self.state.columnNames.length) !== (data.columns && data.columns.length)) {
                    columnsNeedChange = true;
                }
                self.doConstructTableData(datasetTypes.hdfs, data, columnsNeedChange);
            }else {
                renderGlobalErrorMsg(data);
            }
        }
    }

    doConstructTableData(datasetType, data, columnsNeedChange) {
        let tbType = [], tbContentHDFS = [];
        let { tbTitle, columnNames } = this.state;
        let width = datasetModule.getColumnWidth(data.columns.length);
        let tbTitleOnly = datasetModule.getTbTitle(data, width);
        let tbContent = datasetModule.getTbContent(data);

        if(datasetType === datasetTypes.database) {
            tbType = datasetModule.getTbType(data);
            tbTitle = datasetModule.getTbTitleInceptor(
                JSON.parse(JSON.stringify(tbTitleOnly)), width
            );
        }else if(datasetType === datasetTypes.hdfs
            || datasetType === datasetTypes.uploadFile) {

            tbTitle = datasetModule.getTbTitleHDFS(
                JSON.parse(JSON.stringify(tbTitleOnly)),
                this, data.types, true
            );
            tbContentHDFS = [{
                key: '1'
            }];
            if(columnsNeedChange) {
                columnNames = this.constructColumnNames(tbTitle);
            }
        }

        this.setState({
            tbType: tbType,
            tbTitle: tbTitle,
            tbTitleOnly: tbTitleOnly,
            tbContent: tbContent,
            tbContentHDFS: tbContentHDFS,
            columnNames: columnNames
        });
    }

    constructColumnNames(tbTitle) {
        const self = this;
        const tableWidth = this.state.tableWidth;
        const colWidth = datasetModule.getHDFSInputColumnWidth(
            tableWidth, tbTitle.length);

        const columnNames = tbTitle.map((column, index) => {
            return <input
                type="text"
                className={column.type}
                name={column.key}
                key={column.key}
                style={{width: index === 0 && typeof colWidth === 'number' ? colWidth - 2 : colWidth}}
                value={column.title}
                onChange={self.hdfsColumnNameChange}
            />
        });
        return columnNames;
    }

    charsetChange(value, node) {
        let objHDFS = {
            ...this.state.dsHDFS,
            charset: node.props.children
        };
        this.setState({
            dsHDFS: objHDFS
        });
    }

    setDataAccuracy(type, accuracy, column) {
        let tbHDFSTitle = [];
        let tbTitle = this.state.tbTitle;

        if(type.indexOf('varchar') > -1) {
            let maxLen = accuracy.maxLen;
            let varcharStr = 'varchar(' + maxLen + ')';
            tbHDFSTitle = datasetModule.setHDFSColumnType(
                column, tbTitle, varcharStr, this
            );
        }else if(type.indexOf('decimal') > -1) {
            let effectiveNum = accuracy.effectiveNum;
            let decimalNum = accuracy.decimalNum;
            let decimalStr = 'decimal(' + effectiveNum + ',' + decimalNum + ')';
            tbHDFSTitle = datasetModule.setHDFSColumnType(
                column, tbTitle, decimalStr, this
            );
        }else {
            tbHDFSTitle = datasetModule.setHDFSColumnType(
                column, tbTitle, type, this
            );
        }
        const columnNames = this.constructColumnNames(tbHDFSTitle);
        this.setState({
            tbTitle: tbHDFSTitle,
            columnNames: columnNames
        });
    }

    onTypeSelect(value, column) {
        if(value.indexOf('varchar') > -1 || value.indexOf('decimal') > -1) {
            render(
                <DetailType
                    dataType={value}
                    setDataAccuracy={(type, accuracy)=>this.setDataAccuracy(
                        type, accuracy, column)}
                />,
                document.getElementById('popup_root')
            );
        }else {
            this.setDataAccuracy(value, '', column);
        }
    }

    saveHDFSDataset() {
        const dsHDFS = datasetModule.constructHDFSDataset(this.state);
        const opeType = datasetModule.extractOpeType(window.location.hash);
        if(opeType === 'add') {
            this.createHDFS(opeType, dsHDFS);
        }else if(opeType === 'edit') {
            this.editHDFS(opeType, dsHDFS);
        }
    }

    createHDFS(opeType, hdfs) {
        const self = this;
        this.props.createDataset(hdfs, callback);
        function callback(success, data) {
            if(success) {
                let url = '/' + opeType + '/columns/HDFS/';
                self.props.history.push(url);
            }else {
                renderGlobalErrorMsg(data);
            }
        }
    }

    editHDFS(opeType, hdfs) {
        const self = this;
        const hdfsId = datasetModule.getDatasetId(opeType, window.location.hash);
        this.props.editDataset(hdfs, hdfsId, callback);
        function callback(success, data) {
            if(success) {
                let url = '/' + opeType + '/columns/HDFS/' + hdfsId;
                self.props.history.push(url);
            }else {
                renderGlobalErrorMsg(data);
            }
        }
    }

    previewHDFSDataset() {
        const dsHDFS = this.state.dsHDFS;
        this.doFetchHDFSPreviewData(dsHDFS, true);
    }

    render() {
        const {dsHDFS, tableWidth} = this.state;
        const datasetType = datasetModule.extractDatasetType(window.location.hash);
        return (this.state.initDone &&
            <div id="data-preview-id" className="data-preview-box">
                <div className="preview-table">
                    <div
                        className={(datasetType===datasetTypes.hdfs
                            || datasetType===datasetTypes.uploadFile)
                            ?'editable-table-header':'none'}
                        style={{width: tableWidth, height: 40}}
                    >
                        {this.state.columnNames}
                    </div>
                    <div
                        className={datasetType===datasetTypes.database
                            ?'table-header':'none'}
                        style={{width: tableWidth}}
                    >
                        <Table
                            className="data-preview"
                            columns={this.state.tbTitle}
                            dataSource={this.state.tbType}
                            size='small'
                            pagination={false}
                        />
                    </div>
                    <div
                        className={(datasetType===datasetTypes.hdfs
                            || datasetType===datasetTypes.uploadFile)
                            ?'table-header hdfs-table-header':'none'}
                        style={{width: tableWidth}}
                    >
                        <Table
                            columns={this.state.tbTitle}
                            dataSource={this.state.tbContentHDFS}
                            size='small'
                            className="hdfs-data-type"
                            pagination={false}
                        />
                    </div>
                    <div className="table-content" style={{width: tableWidth}}>
                        <Table
                            columns={this.state.tbTitleOnly}
                            dataSource={this.state.tbContent}
                            size='small'
                            pagination={false}
                        />
                    </div>
                </div>
                <div
                    className={datasetType===datasetTypes.database
                        ?'none':'data-detail-preview'}
                >
                    <div className="data-detail-border">
                        <div className="data-detail-item">
                            <span>{intl.get('DATASET.FILE_TYPE')}：</span>
                            <input
                                type="text" value={dsHDFS.file_type}
                                className="tp-input" name="file_type"
                                onChange={this.handleChange}
                            />
                        </div>
                        <div className="data-detail-item">
                            <span>{intl.get('DATASET.SEPARATOR')}：</span>
                            <input
                                type="text" value={dsHDFS.separator}
                                className="tp-input" name="separator"
                                onChange={this.handleChange}
                            />
                        </div>
                        <div className="data-detail-item">
                            <span>{intl.get('DATASET.QUOTE')}：</span>
                            <input
                                type="text" value={dsHDFS.quote}
                                className="tp-input" name="quote"
                                onChange={this.handleChange}
                            />
                        </div>
                        <div className="data-detail-item">
                            <span></span>
                            <div className="data-detail-checkbox">
                                <input
                                    type="checkbox"
                                    className="tp-input"
                                    name="next_as_header"
                                    checked={dsHDFS.next_as_header}
                                    onChange={this.handleChange}
                                />
                                <p>{intl.get('DATASET.FIRST_ROW_AS_COLUMN_NAME')}</p>
                                <Tooltip placement="top" title={intl.get('DATASET.IGNORE_BLANK_LINE')}>
                                    <i
                                        className="icon icon-infor"
                                        style={{marginLeft: '8px'}}
                                    />
                                </Tooltip>
                            </div>
                        </div>
                        <div className="data-detail-item">
                            <span>{intl.get('DATASET.CHARSET')}：</span>
                            <Select
                                style={{width: 312}}
                                value={dsHDFS.charset}
                                onSelect={this.charsetChange}
                            >
                                <Option value="utf-8">utf-8</Option>
                                <Option value="gbk">gbk</Option>
                                <Option value="iso-8859-15">iso-8859-15</Option>
                            </Select>
                        </div>
                    </div>

                    <div className="sub-btn">
                        <button
                            onClick={this.previewHDFSDataset}
                            style={{marginRight: 20}}
                        >
                            {intl.get('DATASET.PREVIEW')}
                        </button>
                        <button onClick={this.saveHDFSDataset}>
                            {intl.get('DATASET.SAVE')}
                        </button>
                    </div>
                    <div id="showAlertPreview" className="alert-tip"></div>
                </div>
            </div>
        );
    }
}

function mapStateToProps (state) {
    const { subDetail } = state;
    return {
        datasetId: subDetail.datasetId,
        dsHDFS: subDetail.dsHDFS,
        dsInceptor: subDetail.dsInceptor,
        isFetching: subDetail.isFetching,
        inceptorPreviewData: subDetail.inceptorPreviewData
    };
}

function mapDispatchToProps (dispatch) {

    //filter out all necessary properties
    const {
        createDataset,
        editDataset,
        saveHDFSDataset,
        saveInceptorPreviewData,
        fetchInceptorPreviewData,
        fetchHDFSPreviewData
    } = bindActionCreators(actionCreators, dispatch);

    return {
        createDataset,
        editDataset,
        saveHDFSDataset,
        saveInceptorPreviewData,
        fetchInceptorPreviewData,
        fetchHDFSPreviewData
    };
}

export default connect (mapStateToProps, mapDispatchToProps)(withRouter(SubPreview));