import React from 'react';
import { render } from 'react-dom';
import { message, Table, Icon } from 'antd';
import { Link } from 'react-router-dom';
import PropTypes from 'prop-types';
import { fetchStateChange, setSelectedRows, fetchSliceDelete, fetchSliceDetail } from '../actions';

class TableChageAuth extends React.Component {
    constructor(props, context) {
        super(props);
        this.dispatch = context.dispatch;
    }

    onSelectChange(selectedRowKeys, selectedRows) {
        let selectedRowNames = [];
        selectedRows.forEach(function(row) {
            selectedRowNames.push(row.name);
        });
        this.props.setSelectedRows(selectedRowKeys, selectedRowNames);
    }

    render() {

        const {files, giveDetail, linkToPath} = this.props;
        const dispatch = this.dispatch;

        const rowSelection = {
            onChange: this.onSelectChange
        };
        const flushDetail = (record) => {
            const {mtime, size, stats, path} = record;
            const {user, group, mode} = stats;
            giveDetail({
                path,
                mtime, //last time modify
                size,
                user,
                group,
                mode
            });
        }
        const columns = [
            {
                title: '名称', //TODO: title need to i18n
                width: '5%',
                render: (text, record) => {
                    const type = record.type;
                    //                        const typesetSelectedRows= Math.random();
                    let datasetType;

                    switch (type) {
                    case 'dir':
                        if (record.name === '.') {
                            datasetType = 'icon-backfile-default';
                        } else if (record.name === '..') {
                            datasetType = 'icon-backarrow-default';
                        } else {
                            datasetType = 'icon-backfile-openfile-warning';
                        }
                        break;
                    case 'file':
                        datasetType = 'icon-grayfile-info';
                        break;
                    default:
                        datasetType = 'icon-grayfile-info';
                        break;
                    }

                    //datasetType = 'icon-disabledfile-info';
                    return (
                        <i className={'icon ' + datasetType}></i>
                    )
                },
                sorter(a, b) {
                    return a.name.substring(0, 1).charCodeAt() - b.name.substring(0, 1).charCodeAt();
                }
            },
            {
                title: '',
                key: 'name',
                dataIndex: 'name',
                width: '24%',
                render: (text, record) => {
                    const name = record.name;
                    if (record.type === 'file') { //go to detail page
                        return (
                            <Link onClick={() => flushDetail(record)} to="/filebrowser">
                                {name === '..' ? name :
                                name === '.' ? ('  ' + name + '  ') : record.path}
                            </Link>
                        );
                    } else { //send request.
                        return (<a onClick={
                            argus => {
                                linkToPath({
                                    path: record.path
                                });
                            }}>{record.path}</a>);
                    }
                }
            }, {
                title: '大小',
                dataIndex: 'size',
                key: 'size',
                width: '16%',
                sorter(a, b) {
                    return a.size - b.size;
                }
            }, {
                title: '用户',
                dataIndex: 'user',
                key: 'user',
                width: '10%',
                sorter(a, b) {
                    return a.stats.user - b.stats.user;
                },
                render: (text, record) => {

                    return (<span>{record.stats ? (record.stats.user || ' ') : ' '}</span>);
                }
            }, {
                title: '组',
                dataIndex: 'group',
                key: 'group',
                width: '10%',
                sorter(a, b) {
                    return a.stats.group - b.stats.group;
                },
                render: (text, record) => {
                    return (<span>{record.stats ? (record.stats.group || ' ') : ' '}</span>);
                }
            }, {
                title: '权限',
                dataIndex: 'rwx',
                key: 'rwx',
                width: '15%',
                render: (text, record) => {
                    return (<span>{record.rwx ? record.rwx : ' '}</span>);
                },
                sorter(a, b) {
                    return a.rwx - b.rwx;
                }
            }, {
                title: '日期',
                dataIndex: 'mtime',
                key: 'mtime',
                width: '25%',
                sorter(a, b) {
                    return a.mtime - b.mtime ? 1 : -1;
                }
            }
        ];

        return (
            <Table
            rowSelection={rowSelection}
            dataSource={files}
            columns={columns}
            pagination={false}
            rowKey={record => record.key}
            />
        );
    }
}

export default TableChageAuth;