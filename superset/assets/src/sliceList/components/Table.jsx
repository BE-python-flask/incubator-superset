import React from 'react';
import {render} from 'react-dom';
import {message, Table, Icon, Tooltip} from 'antd';
import PropTypes from 'prop-types';
import {SliceDelete, SliceEdit} from '../popup';
import {getPermInfo} from '../../perm/actions';
import * as actions from '../actions';
import * as utils from '../../utils/utils';
import intl from 'react-intl-universal';
import { sorterFn } from '../../utils/utils';

class SliceTable extends React.Component {
    constructor(props) {
        super(props);
        this.state = {};

        this.editSlice = this.editSlice.bind(this);
        this.deleteSlice = this.deleteSlice.bind(this);
        this.favoriteSlice = this.favoriteSlice.bind(this);
    }

    onSelectChange(selectedRowKeys, selectedRows) {
        const { dispatch } = this.props;
        let selectedRowNames = [];
        selectedRows.forEach(function(row) {
            selectedRowNames.push(row.slice_name);
        });
        dispatch(actions.setSelectedRows(selectedRowKeys, selectedRowNames));
    };

    editSlice(record) {
        const { dispatch } = this.props;
        dispatch(actions.fetchSliceDetail(record.id, callback));
        function callback(success, data) {
            if(success) {
                render(
                    <SliceEdit
                        dispatch={dispatch}
                        sliceDetail={data}
                    />,
                    document.getElementById('popup_root')
                );
            }else {
                utils.renderGlobalErrorMsg(data);
            }
        }
    }

    deleteSlice(record) {
        const { dispatch } = this.props;
        dispatch(actions.fetchSliceDelInfo(record.id, callback));
        function callback(success, data) {
            if(success) {
                const deleteTips = data;
                render(
                    <SliceDelete
                        dispatch={dispatch}
                        deleteType={'single'}
                        deleteTips={deleteTips}
                        slice={record}
                    />,
                    document.getElementById('popup_root')
                );
            }else {
                utils.renderConfirmModal(data);
            }
        }
    }

    favoriteSlice(record) {
        const { dispatch } = this.props;
        dispatch(actions.fetchStateChange(record, undefined, "favorite"));
    }

    sliceDetail(url) {
        utils.viewObjectDetail(url, callback);
        function callback(success, response) {
            if(success) {
                localStorage.setItem('explore:firstEntry', 'true');
                window.location.href = url;
            }else {
                utils.renderGlobalErrorMsg(response);
            }
        }
    }

    givePerm(record) {
        const callback = (success, response) => {
            if(success) {
                utils.renderPermModal(record.id, record.slice_name, utils.OBJECT_TYPE.SLICE);
            }else {
                utils.renderConfirmModal(response);
            }
        };

        getPermInfo({
            type: utils.OBJECT_TYPE.SLICE,
            id: record.id
        }, callback);
    }

    render() {

        const { sliceList, selectedRowKeys, loading } = this.props;

        const rowSelection = {
            selectedRowKeys,
            onChange: this.onSelectChange.bind(this)
        };

        const columns = [
            {
                title: '',
                dataIndex: 'favorite',
                key: 'favorite',
                width: '2%',
                render: (text, record) => {
                    return (
                        <i className={record.favorite ? 'icon icon-star-fav' : 'icon icon-star'}
                           onClick={() => this.favoriteSlice(record)}/>
                    )
                }
            }, {
                title: intl.get('SLICE.NAME'),
                key: 'slice_name',
                dataIndex: 'slice_name',
                width: '23%',
                render: (text, record) => {
                    return (
                        <div className="entity-name">
                            <div
                                className="entity-title highlight text-overflow-style"
                                style={{maxWidth: 270}}
                            >
                                <a
                                    href="javascript:void(0)"
                                    onClick={argus => this.sliceDetail(record.slice_url)}
                                >
                                    {record.slice_name}
                                </a>
                            </div>
                            <div
                                className="entity-description text-overflow-style"
                                style={{maxWidth: 270}}
                            >
                                {record.description}
                            </div>
                        </div>
                    )
                },
                sorter(a, b) {
                    return sorterFn(a.slice_name, b.slice_name);
                }
            }, {
                title: intl.get('SLICE.CHART_TYPE'),
                dataIndex: 'viz_type',
                key: 'viz_type',
                width: '18%',
                render: (text, record) => {
                    return (
                        <div
                            className="text-overflow-style"
                            style={{maxWidth: 180}}
                            >
                            {record.viz_type}
                        </div>
                    )
                },
                sorter(a, b) {
                    return sorterFn(a.viz_type, b.viz_type);
                }

            }, {
                title: intl.get('SLICE.DATASET'),
                dataIndex: 'datasource',
                key: 'datasource',
                width: '15%',
                render: (text, record) => {
                    return (
                        <a
                            href={record.explore_url}
                            className="highlight text-overflow-style"
                            style={{maxWidth: 160, display: 'inline-block'}}
                        >
                            {record.datasource}
                        </a>
                    )
                },
                sorter(a, b) {
                    return sorterFn(a.datasource, b.datasource);
                }
            }, {
                title: intl.get('SLICE.OWNER'),
                dataIndex: 'created_by_user',
                key: 'created_by_user',
                width: '12%',
                render: (text, record) => {
                    return (
                        <div
                            className="text-overflow-style"
                            style={{maxWidth: 110}}
                        >
                            {record.created_by_user}
                        </div>
                    )
                },
                sorter(a, b) {
                    return sorterFn(a.created_by_user, b.created_by_user);
                }
            }, {
                title: intl.get('SLICE.LAST_MODIFIED_TIME'),
                dataIndex: 'changed_on',
                key: 'changed_on',
                width: '15%',
                sorter(a, b) {
                    return a.changed_time - b.changed_time ;
                }
            }, {
                title: intl.get('SLICE.OPERATION'),
                key: 'action',
                width: '15%',
                render: (record) => {
                    return (
                        <div className="icon-group">
                            <Tooltip placement="top" title={intl.get('SLICE.EDIT')} arrowPointAtCenter>
                                <i
                                    className="icon icon-edit"
                                    style={{position: 'relative', top: 1}}
                                    onClick={() => this.editSlice(record)}
                                />
                            </Tooltip>
                            <Tooltip placement="top" title={intl.get('SLICE.DELETE')} arrowPointAtCenter>
                                <i
                                    className="icon icon-delete"
                                    style={{margin: '0 20'}}
                                    onClick={() => this.deleteSlice(record)}
                                />
                            </Tooltip>
                            <Tooltip placement="top" title={intl.get('SLICE.GRANT_PERM')} arrowPointAtCenter>
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
                dataSource={sliceList}
                columns={columns}
                pagination={false}
                loading={loading}
                rowKey={record => record.id}
            />
        );
    }
}

export default SliceTable;