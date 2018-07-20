import React from 'react';
import { Select, Table } from 'antd';

export function makeSelectOptions(userList) {
    const Option = Select.Option;
    const options = userList.map(
        user => {
            return <Option key={user}>{user}</Option>
        }
    );
    return options;
}

export function makePermCheckboxes(permList, _this, prefix) {
    const perms = permList.map(
        perm => {
            return <div key={perm}>
                    <input
                        id={prefix + perm}
                        type="checkbox"
                        name={perm}
                        onChange={_this.handleChange}
                    />
                    <span>{perm}</span>
                </div>
        }
    );
    return perms;
}

export function makeTableDataSource(data) {
    const dataSource = [];
    for(let attr in data) {
        const userPerm = {
            key: attr,
            name: attr,
            perm: sortPerms(data[attr])
        };
        dataSource.push(userPerm);
    }
    return dataSource;
}

function sortPerms(perms) {
    const sortedPerms = [];
    if(perms.indexOf('READ') > -1) {
        sortedPerms.push('READ');
    }
    if(perms.indexOf('EDIT') > -1) {
        sortedPerms.push('EDIT');
    }
    if(perms.indexOf('ADMIN') > -1) {
        sortedPerms.push('ADMIN');
    }
    return sortedPerms.join(', ');
}

export function makeTableColumns(_this, intl) {
    const columns = [{
        title: intl.get('POPUP.USER_NAME'),
        dataIndex: 'name',
        key: 'name',
        width: '40%'
    }, {
        title: intl.get('POPUP.PERM'),
        dataIndex: 'perm',
        key: 'perm',
        width: '40%'
    }, {
        title: intl.get('POPUP.OPERATION'),
        key: 'action',
        width: '20%',
        render: (text, record) => {
            return (
                <i
                    className="icon icon-delete"
                    onClick={() => _this.revokePerm(record)}
                />
            )
        }
    }];
    return columns;
}