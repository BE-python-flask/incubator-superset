import React, { Component } from 'react';
import { render } from 'react-dom';
import PropTypes from 'prop-types';
import { connect } from 'react-redux';
import {bindActionCreators} from 'redux';

import { Table, Input, Button, Icon, message } from 'antd';

import { TableColumnAdd, TableColumnDelete } from '../popup';
import * as actionCreators from '../actions';
const _ = require('lodash');
import '../style/table.scss';
import intl from 'react-intl-universal';
import {renderGlobalErrorMsg, loadIntlResources} from '../../../utils/utils';

class SubColumns extends Component {
    constructor(props) {
        super(props);
        this.state = {};

        this.deleteTableColumn = this.deleteTableColumn.bind(this);
        this.addTableColumn = this.addTableColumn.bind(this);
        this.onChange = this.onChange.bind(this);
        this.onBlur = this.onBlur.bind(this);
    }

    componentDidMount() {
        const { datasetId, getTableColumn } = this.props;
        if (datasetId) {
            getTableColumn(datasetId);
        }

        loadIntlResources(_ => this.setState({ initDone: true }), 'dataset');
    }

    componentWillReceiveProps(nextProps) {
        const { datasetId, getTableColumn } = this.props;
        if (nextProps.datasetId !== datasetId && nextProps.datasetId) {
            getTableColumn(nextProps.datasetId);
        }
        if(nextProps.tableColumn) {
            this.setState({
                tableColumn: nextProps.tableColumn
            });
        }
    };

    onChange(id, e) {
        const target = e.currentTarget;
        const name = target.name;
        let value = target.value;
        if(target.type === 'checkbox') {
            value = target.checked;
        }

        const state = {...this.state};
        const columns = state.tableColumn;
        const tableColumn = this.updateTableColumn(
            columns,
            id, name, value
        );
        if(target.type === 'checkbox') {
            const column = this.getTableColumn(tableColumn, id);
            this.editTableColumn(column);
        }
        this.setState({
            tableColumn: tableColumn
        });
    }

    onBlur(column) {
        this.formValidate(column);
    }

    updateTableColumn(columns, id, name, value) {
        columns.map(column => {
            if(column.id === id) {
                column[name] = value;
            }
        });
        return columns;
    }

    getTableColumn(columns, id) {
        let selectedColumn = {};
        columns.map(column => {
            if(column.id === id) {
                selectedColumn = column;
            }
        });
        return selectedColumn;
    }

    formValidate(column) {
        if(!(column.column_name && column.column_name.length > 0)) {
            renderGlobalErrorMsg(intl.get('DATASET.METRIC_NOT_NULL'));
            return;
        }
        if(!(column.expression && column.expression.length > 0)) {
            renderGlobalErrorMsg(intl.get('DATASET.EXPRESSION_NOT_NULL'));
            return;
        }
        if(!(column.type && column.type.length > 0)) {
            renderGlobalErrorMsg(intl.get('DATASET.TYPE_NOT_NULL'));
            return;
        }
        if(!this.props.datasetId) {
            renderGlobalErrorMsg(intl.get('DATASET.ID_NOT_NULL'));
            return;
        }
        this.editTableColumn(column);
    }

    editTableColumn(column) {
        column.dataset_id = this.props.datasetId;
        this.props.fetchTableColumnEdit(
            column,
            this.editCallback
        );
    }

    editCallback(success, message) {
        if(!success) {
            message.warning(message, 5);
        }
    }

    addTableColumn () {
        const { fetchTableColumnAdd } = this.props;
        render(
            <TableColumnAdd
                title="添加列"
                datasetId={this.props.datasetId}
                fetchTableColumnAdd={fetchTableColumnAdd}
            />,
            document.getElementById('popup_root')
        );
    }

    deleteTableColumn(record) {
        const { fetchTableColumnDelete } = this.props;
        let deleteTips = intl.get('DATASET.CONFIRM') + intl.get('DATASET.DELETE') + record.column_name + "?";
        render(
            <TableColumnDelete
                fetchTableColumnDelete={fetchTableColumnDelete}
                column={record}
                deleteTips={deleteTips}>
            </TableColumnDelete>,
            document.getElementById('popup_root')
        );
    }
 
    render() {
        const me = this;

        let data = [];

        _.forEach(me.state.tableColumn, function(item, index) {
            item.key = index;
            data.push(item);
        });

        const columns = [{
            title: intl.get('DATASET.COLUMN'),
            dataIndex: 'column_name',
            key: 'column_name',
            width: '15%',
            className: 'text-column',
            render: (text, record) => {
                return (
                    <input
                        name="column_name"
                        className="tp-input columns-input"
                        value={record.column_name}
                        onChange={(e) => this.onChange(record.id, e)}
                        onBlur={(args) => this.onBlur(record)}
                    />
                )
            }
        }, {
            title: intl.get('DATASET.EXPRESSION'),
            dataIndex: 'expression',
            key: 'expression',
            width: '15%',
            className: 'text-column',
            render: (text, record) => {
                return (
                    <input
                        name="expression"
                        className="tp-input columns-input"
                        value={record.expression}
                        onChange={(e) => this.onChange(record.id, e)}
                        onBlur={(args) => this.onBlur(record)}
                    />
                )
            }
        }, {
            title: intl.get('DATASET.TYPE'),
            dataIndex: 'type',
            key: 'type',
            width: '10%',
            className: 'text-column',
            render: (text, record) => {
                return (
                    <input
                        name="type"
                        className="tp-input columns-input"
                        value={record.type}
                        onChange={(e) => this.onChange(record.id, e)}
                        onBlur={(args) => this.onBlur(record)}
                    />
                )
            }
        }, {
            title: intl.get('DATASET.GROUP'),
            dataIndex: 'groupAble',
            key: 'groupAble',
            width: '7%',
            className: 'checkb',
            render: (text, record) => {
                return (
                    <input
                        name="groupby"
                        type="checkbox"
                        checked={record.groupby}
                        onChange={(e) => this.onChange(record.id, e)}
                    />
                )
            }
        }, {
            title: intl.get('DATASET.FILTER'),
            dataIndex: 'filterAble',
            key: 'filterAble',
            width: '7%',
            className: 'checkb',
            render: (text, record) => {
                return (
                    <input
                        name="filterable"
                        type="checkbox"
                        checked={record.filterable}
                        onChange={(e) => this.onChange(record.id, e)}
                    />
                )
            }
        }, {
            title: intl.get('DATASET.COUNT'),
            dataIndex: 'accountAble',
            key: 'accountAble',
            width: '7%',
            className: 'checkb',
            render: (text, record) => {
                return (
                    <input
                        name="count_distinct"
                        type="checkbox"
                        checked={record.count_distinct}
                        onChange={(e) => this.onChange(record.id, e)}
                    />
                )
            }
        },{
            title: intl.get('DATASET.SUM'),
            dataIndex: 'sumAble',
            key: 'sumAble',
            width: '7%',
            className: 'checkb',
            render: (text, record) => {
                return (
                    <input
                        name="sum"
                        type="checkbox"
                        checked={record.sum}
                        onChange={(e) => this.onChange(record.id, e)}
                    />
                )
            }
        },{
            title: intl.get('DATASET.AVERAGE'),
            dataIndex: 'avgAble',
            key: 'avgAble',
            width: '7%',
            className: 'checkb',
            render: (text, record) => {
                return (
                    <input
                        name="avg"
                        type="checkbox"
                        checked={record.avg}
                        onChange={(e) => this.onChange(record.id, e)}
                    />
                )
            }
        },{
            title: intl.get('DATASET.MIN'),
            dataIndex: 'minimumSeekAble',
            key: 'minimumSeekAble',
            width: '7%',
            className: 'checkb',
            render: (text, record) => {
                return (
                    <input
                        name="min"
                        type="checkbox"
                        checked={record.min}
                        onChange={(e) => this.onChange(record.id, e)}
                    />
                )
            }
        },{
            title: intl.get('DATASET.MAX'),
            dataIndex: 'maximumSeekAble',
            key: 'maximumSeekAble',
            width: '7%',
            className: 'checkb',
            render: (text, record) => {
                return (
                    <input
                        name="max"
                        type="checkbox"
                        checked={record.max}
                        onChange={(e) => this.onChange(record.id, e)}
                    />
                )
            }
        },{
            title: intl.get('DATASET.SHOW_TIME'),
            dataIndex: 'timeExpressAble',
            key: 'timeExpressAble',
            width: '7%',
            className: 'checkb',
            render: (text, record) => {
                return (
                    <input
                        name="is_dttm"
                        type="checkbox"
                        checked={record.is_dttm}
                        onChange={(e) => this.onChange(record.id, e)}
                    />
                )
            }
        },{
            title: intl.get('DATASET.OPERATION'),
            dataIndex: 'operation',
            key: 'operation',
            width: '4%',
            render: (text, record, index) => {
                return (
                    <div className="icon-group" style={{display: 'flex'}}>
                        <i className="icon icon-delete"
                            onClick={() => this.deleteTableColumn(record)}
                        />
                    </div>
                )
            }
        }];

        return (
            <div className="list-table-column" style={{padding: '10px'}}>
                <div style={{width:'100%', height:'50px', textAlign:'right'}}>
                    <button
                        onClick={this.addTableColumn}
                        className='btn-blue tab-btn-ps'
                        disabled={this.props.datasetId === '' ? true : false}
                    >+&nbsp; {intl.get('DATASET.ADD')}</button>
                </div>
                <Table
                    columns={columns}
                    dataSource={data}
                    size='small'
                    pagination={false}
                />
            </div>
        );
    }
}

function mapStateToProps(state) {
    const { tabData, subDetail } = state;

    return {
        tableColumn: tabData.tableColumn.data,
        datasetId:  subDetail.datasetId
    };
}

function mapDispatchToProps(dispatch) {
    const {
        getTableColumn,
        fetchTableColumnAdd,
        fetchTableColumnDelete,
        fetchTableColumnEdit
    } = bindActionCreators(actionCreators, dispatch);

    return  {
        getTableColumn,
        fetchTableColumnAdd,
        fetchTableColumnDelete,
        fetchTableColumnEdit
    };
}


export default connect(mapStateToProps, mapDispatchToProps)(SubColumns);