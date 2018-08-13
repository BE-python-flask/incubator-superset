import React from 'react';
import { render } from 'react-dom';
import { message, Table, Icon } from 'antd';
import { Link } from 'react-router-dom';
import PropTypes from 'prop-types';
import { fetchStateChange, setSelectedRows, fetchSliceDelete, fetchSliceDetail } from '../actions';
import { sortHDFSFiles } from '../module';
import style from '../style/hdfs.scss'
import intl from "react-intl-universal";
import { sorterFn } from './../../utils/utils';

class InnerTable extends React.Component {
    constructor(props, context) {
        super(props);
        this.dispatch = context.dispatch;

        this.onSelectChange = this.onSelectChange.bind(this);
        this.clearTableState = this.clearTableState.bind(this);
    }

    onSelectChange(selectedRowKeys, selectedRows) {
        let length = selectedRows.length;
        let selectedRow = selectedRows[length - 1];

        if (!selectedRow) {
            this.props.setSelectedRows({
                selectedRows: [],
                selectedRowKeys: [],
                selectedRowNames: []
            });
            return;
        }
        let selectedRowNames = [];

        selectedRows.forEach(function(row) {
            selectedRowNames.push('"' + row.name + '"');
        });

        this.props.setSelectedRows(selectedRows, selectedRowKeys, selectedRowNames);
    }

    clearTableState() {
        this.setState({
            selectedRowKeys: []
        })
    }

    render() {
        const {selectedRowKeys} = this.props.condition;
        const {files, giveDetail, linkToPath, fetchIfNeeded} = this.props;
        let hdfsFiles = sortHDFSFiles(files);

        const rowSelection = {
            selectedRowKeys,
            onChange: this.onSelectChange
        };
        const flushDetail = (record) => {
            const {mtime, size, stats, path, name} = record;
            const {user, group, mode} = stats;
            giveDetail({
                name,
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
                title: intl.get('name'), //TODO: title need to i18n
                width: '5%',
                render: (text, record) => {
                    const type = record.type;
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
                    if (record.type === 'file') {
                        return (
                            <Link onClick={() => flushDetail(record)} to="/filebrowser">
                                <i className={'icon ' + datasetType}></i>
                            </Link>
                        );
                    } else {
                        return (
                            <a onClick={argus => {
                                linkToPath({
                                    path: record.path
                                });
                            }}>
                                <i className={'icon ' + datasetType}></i>
                            </a>);
                    }
                },
                sorter(a, b) {
                    return sorterFn(a.name, b.name);
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
                                {name === '.' ? ('  ' + name + '  ') : name}
                            </Link>
                        );
                    } else { //send request.
                        return (
                            <a
                            onClick={
                            argus => {
                                linkToPath({
                                    path: record.path
                                });
                            }
                            }
                            className={record.name === '.' || record.name === '..' ? 'folder-sign' : ''}
                            >{name}</a>
                        );
                    }
                }
            }, {
                title: intl.get('size'),
                dataIndex: 'size',
                key: 'size',
                width: '16%',
                sorter(a, b) {
                    let aa = +a.size.replace(/[a-zA-Z]/g, '');
                    let bb = +b.size.replace(/[a-zA-Z]/g, '');

                    return aa - bb;
                }
            }, {
                title: intl.get('users'),
                dataIndex: 'user',
                key: 'user',
                width: '10%',
                sorter(a, b) {
                    return sorterFn(a.stats.user, b.stats.user);
                },
                render: (text, record) => {
                    return (<span>{record.stats ? (record.stats.user || ' ') : ' '}</span>);
                }
            }, {
                title: intl.get('group'),
                dataIndex: 'group',
                key: 'group',
                width: '10%',
                sorter(a, b) {
                    return sorterFn(a.stats.group, b.stats.group);
                },
                render: (text, record) => {
                    return (<span>{record.stats ? (record.stats.group || ' ') : ' '}</span>);
                }
            }, {
                title: intl.get('authority'),
                dataIndex: 'rwx',
                key: 'rwx',
                width: '15%',
                render: (text, record) => {
                    return (<span>{record.rwx ? record.rwx : ' '}</span>);
                },
                sorter(a, b) {
                    return sorterFn(a.rwx, b.rwx);
                }
            }, {
                title: intl.get('date'),
                dataIndex: 'mtime',
                key: 'mtime',
                width: '25%',
                sorter(a, b) {
                    return a.stats.mtime - b.stats.mtime;
                }
            }
        ];

        return (
            <Table
            rowSelection={rowSelection}
            dataSource={hdfsFiles}
            columns={columns}
            pagination={false}
            rowKey={record => record.key}
            />
        );
    }
}

export default InnerTable;