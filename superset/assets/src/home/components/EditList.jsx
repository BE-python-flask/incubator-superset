import React, { Component } from 'react';
import PropTypes from 'prop-types';
import { Table, Tooltip } from 'antd';
import { renderGlobalErrorMsg, viewObjectDetail } from '../../utils/utils';
import intl from "react-intl-universal";

function Edit(props) {
    return (
        <Table dataSource={props.dataSource} columns={props.columns} />
    );
}

Edit.propTypes = {
    name: PropTypes.string.isRequired,
    time: PropTypes.string.isRequired,
};

export default class EditList extends Component {
    constructor(props) {
        super();
        this.translateOperation = this.translateOperation.bind(this);
    }

    translateOperation(type) {
        if(type === 'edit') {
            return intl.get('edit');
        }else if('create') {
            return intl.get('create');
        }else {
            return type;
        }
    }

    viewHomeDetail(url) {
        viewObjectDetail(url, callback);
        function callback(success, response) {
            if(success) {
                window.location.href = url;
            }else {
                renderGlobalErrorMsg(response);
            }
        }
    }

    render() {
        const listDashboard = this.props.dashboard;
        const listSlice = this.props.slice;

        const dataSource = (this.props.catagory === 'dashboard'? listDashboard : listSlice) || [];
        const columns = [{
            title: intl.get('name'),
            dataIndex: 'name',
            key: 'name',
            className: "name-column",
            sorter: (a, b) => a.name.localeCompare(b.name),
            render: (text, record) => (
                <Tooltip placement="topLeft" title={text} arrowPointAtCenter>
                    <a
                        href="javascript:void(0)"
                        onClick={() => this.viewHomeDetail(record.link)}
                    >{text}</a>
                </Tooltip>)
        }, {
            title: intl.get('action'),
            dataIndex: 'action',
            key: 'action',
            className: "action-column",
            sorter: (a, b) => a.action.localeCompare(b.action),
            render: (text) => (<span>{this.translateOperation(text)}</span>)
        }, {
            title: intl.get('edit_time'),
            dataIndex: 'time',
            key: 'time',
            className: "time-column",
            sorter: (a, b) => { return a.time > b.time　? 1 : -1;},
            render: (text) => (<span>{text}</span>)
        }];

        return (
            <Table dataSource={dataSource} pagination={false} columns={columns} />
        );
    }
}
