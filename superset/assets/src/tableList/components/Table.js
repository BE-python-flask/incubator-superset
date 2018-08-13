import React from 'react';
import { render } from 'react-dom';
import { Link } from 'react-router-dom';
import { Table, Tooltip } from 'antd';
import PropTypes from 'prop-types';
import { datasetTypes } from '../actions';
import * as actions from '../actions';
import { TableDelete } from '../popup';
import * as utils from '../../../utils/utils';
import intl from 'react-intl-universal';
import { getPermInfo } from '../../perm/actions';

class SliceTable extends React.Component {
    constructor(props) {
        super(props);
        this.state = {};
    }

    onSelectChange(selectedRowKeys, selectedRows) {
        const {dispatch} = this.props;
        let selectedRowNames = [];
        selectedRowKeys = [];
        selectedRows.forEach(function(row) {
            selectedRowNames.push(row.dataset_name);
            selectedRowKeys.push(row.id);
        });
        dispatch(actions.selectRows(selectedRowKeys, selectedRowNames));
    }

    givePerm(record) {
        const callback = (success, response) => {
            if (success) {
                utils.renderPermModal(record.id, record.dataset_name, utils.OBJECT_TYPE.DATASET);
            } else {
                utils.renderConfirmModal(response);
            }
        };

        getPermInfo({
            type: utils.OBJECT_TYPE.TABLE,
            id: record.id
        }, callback);
    }

    viewTableDetail(url) {
        utils.viewObjectDetail(url, callback);
        function callback(success, response) {
            if (success) {
                window.location.href = url;
            } else {
                utils.renderGlobalErrorMsg(response);
            }
        }
    }

    render() {

        const {dispatch, data, selectedRowKeys} = this.props;

        function editTable(record) {
            dispatch(actions.switchDatasetType(record.dataset_type));
            dispatch(actions.saveDatasetId(record.id));
            dispatch(actions.clearDatasetData());
        }

        function deleteTable(record) {
            dispatch(actions.fetchTableDelInfo(record.id, callback));
            function callback(success, data) {
                if (success) {
                    let deleteTips = data;
                    render(
                        <TableDelete
                        dispatch={dispatch}
                        deleteType={'single'}
                        deleteTips={deleteTips}
                        table={record} />,
                        document.getElementById('popup_root')
                    );
                } else {
                    utils.renderConfirmModal(data);
                }
            }
        }

        const rowSelection = {
            selectedRowKeys,
            onChange: this.onSelectChange.bind(this)
        };

        const columns = [
            {
                width: '2%',
                render: (text, record) => {
                    return (
                        <i className={'icon ' + record.iconClass} />
                    )
                }
            },
            {
                title: intl.get('DATASET.NAME'),
                key: 'datasetName',
                dataIndex: 'dataset_name',
                width: '33%',
                render: (text, record) => {
                    return (
                        <div className="entity-name">
                            <div
                        className="entity-title highlight text-overflow-style"
                        style={{
                            maxWidth: 370
                        }}
                        >
                                <a
                        href="javascript:void(0)"
                        target="_blank"
                        onClick={() => this.viewTableDetail(record.explore_url)}
                        >
                                    {record.dataset_name}
                                </a>
                            </div>
                            <div
                        className="entity-description text-overflow-style"
                        style={{
                            maxWidth: 370
                        }}
                        >
                                {record.dataset_type} | {record.connection}
                            </div>
                        </div>
                    )
                },
                sorter(a, b) {
                    return utils.sortByInitials(a.dataset_name, b.dataset_name);
                }
            }, {
                title: intl.get('DATASET.OWNER'),
                dataIndex: 'created_by_user',
                key: 'created_by_user',
                width: '25%',
                render: (text, record) => {
                    return (
                        <div
                        className="text-overflow-style"
                        style={{
                            maxWidth: 270
                        }}
                        >
                            {record.created_by_user}
                        </div>
                    )
                },
                sorter(a, b) {
                    return utils.sortByInitials(a.created_by_user, b.created_by_user);
                }
            }, {
                title: intl.get('DATASET.UPDATE_TIME'),
                dataIndex: 'changed_on',
                key: 'changed_on',
                width: '25%',
                sorter(a, b) {
                    return a.changed_on - b.changed_on ? 1 : -1;
                }
            }, {
                title: intl.get('DATASET.OPERATION'),
                key: 'action',
                width: '15%',
                render: (record) => {
                    return (
                        <div className="icon-group">
                            <Tooltip placement="top" title={intl.get('DATASET.EDIT')} arrowPointAtCenter>
                                <Link
                        onClick={() => editTable(record)}
                        style={{
                            position: 'relative',
                            top: 1
                        }}
                        to={`/edit/detail/${record.dataset_type === datasetTypes.hdfs ? datasetTypes.hdfs : datasetTypes.database}/${record.id}`}
                        >
                                    <i className="icon icon-edit"/>
                                </Link>
                            </Tooltip>
                            <Tooltip placement="top" title={intl.get('DATASET.DELETE')} arrowPointAtCenter>
                                <i
                        className="icon icon-delete"
                        style={{
                            margin: '0 20'
                        }}
                        onClick={() => deleteTable(record)}
                        />
                            </Tooltip>
                            <Tooltip placement="top" title={intl.get('DATASET.GRANT_PERM')} arrowPointAtCenter>
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
            rowKey={record => record.id}
            />
        );
    }
}

export default SliceTable;