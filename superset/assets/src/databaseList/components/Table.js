import React from 'react';
import { render } from 'react-dom';
import { message, Table, Icon, Tooltip } from 'antd';
import PropTypes from 'prop-types';
import { connectionTypes } from '../actions';
import * as actions from '../actions';
import * as utils from '../../utils/utils';
import intl from 'react-intl-universal';
import { ConnectionDelete, ConnectionEdit } from '../popup';
import { isCorrectConnection } from '../utils';

import { getPermInfo } from '../../perm/actions';

class SliceTable extends React.Component {
    constructor(props, context) {
        super(props);
        this.state = {};
        //bindings
        this.deleteConnection = this.deleteConnection.bind(this);
        this.editConnection = this.editConnection.bind(this);

        this.dispatch = context.dispatch;
    }

    onSelectChange(selectedRowKeys, selectedRows) {
        let selectedRowNames = [];
        let connToBeDeleted = {};
        selectedRows.map(function(row) {
            let connectionType = connectionTypes.hdfs;
            if (isCorrectConnection(row.connection_type, connectionTypes)) {
                connectionType = connectionTypes.database;
            }
            if (connToBeDeleted[connectionType]) {
                connToBeDeleted[connectionType].push(row.id);
            } else {
                connToBeDeleted[connectionType] = [row.id];
            }
            selectedRowNames.push(row.name);
        });
        this.dispatch(actions.selectRows(selectedRowKeys, connToBeDeleted, selectedRowNames));
    }

    editConnection(record) {
        const dispatch = this.dispatch;
        dispatch(actions.fetchDBDetail(record, callback));

        function callback(success, data) {
            if (success) {
                render(
                    <ConnectionEdit
                    dispatch={dispatch}
                    connectionType={record.connection_type}
                    connectionId={record.id}
                    database={data} />,
                    document.getElementById('popup_root')
                );
            } else {
                utils.renderGlobalErrorMsg(data);
            }
        }
    }

    deleteConnection(record) {
        const dispatch = this.dispatch;
        dispatch(actions.fetchConnectDelInfo(record, callback));
        function callback(success, data) {
            if (success) {
                let deleteTips = data;
                let connToBeDeleted = {};
                let connectionType = connectionTypes.hdfs;
                if (isCorrectConnection(record.connection_type, connectionTypes)) {
                    connectionType = connectionTypes.database;
                }
                connToBeDeleted[connectionType] = [record.id];
                dispatch(actions.selectRows([record.elementId], connToBeDeleted, [record.name]));

                render(
                    <ConnectionDelete
                    dispatch={dispatch}
                    deleteTips={deleteTips}
                    connection={record}/>,
                    document.getElementById('popup_root')
                );
            } else {
                utils.renderConfirmModal(data);
            }
        }
    }

    givePerm(record) {
        const objectType = isCorrectConnection(record.connection_type, connectionTypes)
            ? utils.OBJECT_TYPE.DATABASE : utils.OBJECT_TYPE.HDFSCONNECTION;
        const callback = (success, response) => {
            if (success) {
                utils.renderPermModal(record.id, record.name, objectType);
            } else {
                utils.renderConfirmModal(response);
            }
        };

        getPermInfo({
            type: objectType,
            id: record.id
        }, callback);
    }

    render() {
        const {data, selectedRowKeys} = this.props;
        const rowSelection = {
            selectedRowKeys,
            onChange: this.onSelectChange.bind(this)
        };

        const columns = [
            {
                title: intl.get('DATABASE.NAME'),
                key: 'name',
                dataIndex: 'name',
                width: '25%',
                render: (text, record) => {
                    return (
                        <div className="entity-name">
                            <div
                        className="entity-title text-overflow-style"
                        style={{
                            maxWidth: 290
                        }}
                        >
                                {record.name}
                            </div>
                        </div>
                    )
                },
                sorter(a, b) {
                    return utils.sortByInitials(a.name, b.name);
                }
            }, {
                title: intl.get('DATABASE.CONN_TYPE'),
                dataIndex: 'connection_type',
                key: 'connection_type',
                width: '20%',
                sorter(a, b) {
                    return utils.sortByInitials(a.connection_type, b.connection_type);
                }
            }, {
                title: intl.get('DATABASE.OWNER'),
                dataIndex: 'owner',
                key: 'owner',
                width: '15%',
                render: (text, record) => {
                    return (
                        <div
                        className="text-overflow-style"
                        style={{
                            maxWidth: 170
                        }}
                        >
                            {record.owner}
                        </div>
                    )
                },
                sorter(a, b) {
                    return utils.sortByInitials(a.owner, b.owner);
                }
            }, {
                title: intl.get('DATABASE.UPDATE_TIME'),
                dataIndex: 'changed_on',
                key: 'changed_on',
                width: '20%',
                sorter(a, b) {
                    return a.changed_on - b.changed_on ? 1 : -1;
                }
            }, {
                title: intl.get('DATABASE.OPERATION'),
                key: 'action',
                width: '15%',
                render: (record) => {
                    return (
                        <div className="icon-group">
                            <Tooltip placement="top" title={intl.get('DATABASE.EDIT')} arrowPointAtCenter>
                                <i
                        className="icon icon-edit"
                        style={{
                            position: 'relative',
                            top: 1
                        }}
                        onClick={() => this.editConnection(record)}
                        />
                            </Tooltip>
                            <Tooltip placement="top" title={intl.get('DATABASE.DELETE')} arrowPointAtCenter>
                                <i
                        className="icon icon-delete"
                        style={{
                            margin: '0 20'
                        }}
                        onClick={() => this.deleteConnection(record)}
                        />
                            </Tooltip>
                            <Tooltip placement="top" title={intl.get('DATABASE.GRANT_PERM')} arrowPointAtCenter>
                                <i
                        className="icon icon-perm"
                        onClick={() => this.givePerm(record)}
                        />
                            </Tooltip>
                        </div>
                    )
                }
            }
        ];

        return (
            <Table
            rowSelection={rowSelection}
            dataSource={data}
            columns={columns}
            pagination={false}
            rowKey={record => record.elementId}
            />
        );
    }
}

SliceTable.contextTypes = {
    dispatch: PropTypes.func.isRequired
};

export default SliceTable;