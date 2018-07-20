import React from 'react';
import PropTypes from 'prop-types';
import { Table, Tooltip } from 'antd';
import { renderGlobalErrorMsg, viewObjectDetail } from '../../utils/utils';
import intl from "react-intl-universal";

function EventList(props) {
    const viewHomeDetail = (url)=> {
        viewObjectDetail(url, callback);
        function callback(success, response) {
            if(success) {
                window.location.href = url;
            }else {
                renderGlobalErrorMsg(response);
            }
        }
    };

    const dataSource = props.eventList || [];
    const columns = [{
        title: intl.get('users'),
        dataIndex: 'user',
        key: 'user',
        className: "user-column",
        sorter: (a, b) => a.user.localeCompare(b.user),
        render: (text, record) => (
            <Tooltip placement="topLeft" title={text} arrowPointAtCenter>
                <a
                    className="user-td"
                    style={{display: 'inline-block'}}
                    href="javascript:void(0)"
                    onClick={() => viewHomeDetail('/present_user')}
                >
                    {/*<i className="icon user-icon" />*/}
                    <span>{text}</span>
                </a>
            </Tooltip>
        )
    }, {
        title: intl.get('action'),
        dataIndex: 'action',
        key: 'action',
        sorter: (a, b) => a.action.localeCompare(b.action),
        className: "action-column",
        render: (text, record) => {
            {/*const classes = "icon action-title-icon " + record.type + "-icon";*/}
            return (
                <div>
                    <div className="action-text">
                        <Tooltip placement="topRight" title={text} arrowPointAtCenter>{text}</Tooltip>
                    </div>
                    {/*<div className="action-title">
                        <i className={classes}/>
                        <span>{record.title}</span>
                    </div>*/}
                </div>
            );
        }
    }, {
        title: intl.get('edit_time'),
        dataIndex: 'time',
        key: 'time',
        sorter: (a, b) => { return a.time > b.time　? 1 : -1;},
        className: "time-column",
        render: (text) => (<span>{text}</span>)
    }];

    return (
        <Table dataSource={dataSource} pagination={false} columns={columns} />
    );
}

EventList.propTypes = {
    eventList: PropTypes.array.isRequired,
};

export default EventList;