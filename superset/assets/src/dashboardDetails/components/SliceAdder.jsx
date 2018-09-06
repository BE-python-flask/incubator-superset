import $ from 'jquery';
import React, { PropTypes } from 'react';
import ReactDOM from 'react-dom';
import { BootstrapTable, TableHeaderColumn } from 'react-bootstrap-table';
import ModalTrigger from '../../components/ModalTrigger';
import { Table, Pagination } from 'antd';
require('react-bootstrap-table/css/react-bootstrap-table.css');
import {sortByInitials} from '../../utils/utils';
import intl from 'react-intl-universal';

const propTypes = {
    dashboard: PropTypes.object.isRequired,
    triggerNode: PropTypes.node.isRequired,
};

class SliceAdder extends React.Component {
    constructor(props) {
        super(props);
        this.state = {
            slices: [],
            pageSize: 10000,
            pageNumber: 1,
            slicesLoaded: false,
            selectedRowKeys: {},
            selectedRows: {},
            enableAddSlice: false
        };

        this.addSlices = this.addSlices.bind(this);
        this.onPageChange = this.onPageChange.bind(this);
        this.keywordChange = this.keywordChange.bind(this);
    }

    keywordChange(event) {
        const keyword = event.target.value;
        this.getSliceList(1, keyword);
    }

    onChange(selectedRowKeys) {//for show selected state in table
        let _selectedKeys = {...this.state.selectedRowKeys};
        _selectedKeys[this.state.pageNumber] = selectedRowKeys;
        const enableAddSlice = this.judgeEnableAddSlice(_selectedKeys);
        this.setState({
            selectedRowKeys: _selectedKeys,
            enableAddSlice: enableAddSlice
        });
    };

    onSelect(record, selected) {//for make selected ids
        let _selectedRows = {...this.state.selectedRows};
        if(selected) {
            _selectedRows[record.id] = record;
        }else {
            delete _selectedRows[record.id];
        }
        this.setState({
            selectedRows: _selectedRows
        });
    };

    judgeEnableAddSlice(selectedKeys) {
        for(let attr in selectedKeys) {
            if(selectedKeys[attr] && selectedKeys[attr].length > 0) {
                return true;
            }
        }
        return false;
    }

    onPageChange(page) {
        this.setState({
            pageNumber: page
        });
    }

    addSlices() {
        let selectedSliceIds = [];
        const selectedRows = this.state.selectedRows;
        for(let id in selectedRows) {
            selectedSliceIds.push(id);
        }
        this.props.dashboard.addSlicesToDashboard(selectedSliceIds);
    }

    getSliceList(pageNumber, keyword) {
        const self = this;
        const url = '/slice/listdata/?page=' + (pageNumber-1) + '&page_size=' + self.state.pageSize + '&filter=' + keyword;
        this.slicesRequest = $.ajax({
            url: url,
            type: 'GET',
            success: response => {
                const slices = response.data.data;
                const sliceCount = response.data.count;
                const totalPage = Math.ceil(sliceCount/self.state.pageSize);
                self.setState({
                    slices,
                    totalPage: totalPage,
                    pageNumber: pageNumber,
                    slicesLoaded: true
                });
            },
            error: error => {
                self.errored = true;
                self.setState({
                    errorMsg: self.props.dashboard.getAjaxErrorMsg(error),
                });
            }
        });
    }

    componentDidMount() {
        //first load list
        this.getSliceList(1, '');
    }

    componentWillUnmount() {
        this.slicesRequest.abort();
    }

    render() {
        const { selectedRowKeys, pageNumber, enableAddSlice } = this.state;
        const hideLoad = this.state.slicesLoaded || this.errored;

        const rowSelection = {
            selectedRowKeys: selectedRowKeys[pageNumber],
            onSelect: this.onSelect.bind(this),
            onChange: this.onChange.bind(this)
        };

        const columns = [
            {
                title: intl.get('DASHBOARD.NAME'),
                key: 'slice_name',
                dataIndex: 'slice_name',
                width: '40%',
                render: (text, record) => {
                    return (
                        <div className="entity-name">
                            <div className="entity-title">
                                {record.slice_name}
                            </div>
                            <div
                                className="entity-description"
                                style={{
                                    textOverflow:'ellipsis',
                                    whiteSpace:'nowrap',
                                    overflow:'hidden',
                                    maxWidth:'230px'
                                }}
                            >
                                {record.description}
                            </div>
                        </div>
                    )
                },
                sorter(a, b) {
                    return sortByInitials(a.slice_name, b.slice_name);
                }
            }, {
                title: intl.get('DASHBOARD.CHART_TYPE'),
                dataIndex: 'viz_type',
                key: 'viz_type',
                width: '20%',
                sorter(a, b) {
                    return sortByInitials(a.viz_type, b.viz_type);
                }
            }, {
                title: intl.get('DASHBOARD.DATASET'),
                dataIndex: 'datasource',
                key: 'datasource',
                width: '20%',
                sorter(a, b) {
                    return sortByInitials(a.datasource, b.datasource);
                }
            }, {
                title: intl.get('DASHBOARD.OWNER'),
                dataIndex: 'created_by_user',
                key: 'created_by_user',
                width: '20%',
                sorter(a, b) {
                    return sortByInitials(a.created_by_user, b.created_by_user);
                }
            }
        ];

        const modalTitle = intl.get('DASHBOARD.ADD_SLICE');
        const modalIcon = "icon icon-plus";
        const modalContent = (
            <div className="table-add-slice">
                <img
                    src="/static/assets/images/loading.gif"
                    className={'loading ' + (hideLoad ? 'hidden' : '')}
                    alt={hideLoad ? '' : 'loading'}
                />
                <div className={this.errored ? '' : 'hidden'}>
                    {this.state.errorMsg}
                </div>
                <div className={this.state.slicesLoaded ? '' : 'hidden'}>
                    <div className="search-input">
                        <input
                            className="tp-input"
                            onChange={this.keywordChange}
                            placeholder={intl.get('DASHBOARD.SEARCH')}
                        />
                        <i className="icon icon-search"/>
                    </div>
                    <Table
                        rowSelection={rowSelection}
                        dataSource={this.state.slices}
                        columns={columns}
                        pagination={{
                            pageSize: 8,
                            onChange: this.onPageChange
                        }}
                    />
                </div>
            </div>
        );
        const modalFooter = (
            <div>
                <button
                    type="button"
                    className="tp-btn tp-btn-middle tp-btn-primary"
                    data-dismiss="modal"
                    onClick={this.addSlices}
                    disabled={!enableAddSlice}
                >
                    添加
                </button>
            </div>
        );

        return (
            <ModalTrigger
                triggerNode={this.props.triggerNode}
                isButton
                className='popup-modal-add-slice'
                modalBody={modalContent}
                modalIcon={modalIcon}
                modalTitle={modalTitle}
                modalFooter={modalFooter}
            />
        );
    }
}

SliceAdder.propTypes = propTypes;

export default SliceAdder;
