import React from 'react';
import intl from "react-intl-universal";

export function getPermColumns(_this) {

    const columns = [{
        title: '',
        dataIndex: 'name',
        key: 'name',
        width: '20%'
    }, {
        title: intl.get('users'),
        dataIndex: 'user',
        key: 'user',
        width: '20%',
        className: 'checkb',
        render: (text, record) => {
            return (<input type="checkbox" onClick={() => _this.onPermChange(record, 'user')} checked={record.user} />)
        }
    }, {
        title: intl.get('group'),
        dataIndex: 'group',
        key: 'group',
        width: '20%',
        className: 'checkb',
        render: (text, record) => {
            return (<input type="checkbox" onClick={() => _this.onPermChange(record, 'group')} checked={record.group} />)
        }
    }, {
        title: intl.get('others'),
        dataIndex: 'other',
        key: 'other',
        width: '20%',
        className: 'checkb',
        render: (text, record) => {
            return (<input type="checkbox" onClick={() => _this.onPermChange(record, 'other')} checked={record.other} />)
        }
    }];

    return columns;
}

export function getPermData(hdfsItem) {
    const perms = analysisPerms(hdfsItem[0].rwx);
    if(!perms) {
        return;
    }
    const permData = [{
        key: 'perm1',
        name: intl.get('read'),
        user: perms[0][0],
        group: perms[0][1],
        other: perms[0][2]
    }, {
        key: 'perm2',
        name: intl.get('write'),
        user: perms[1][0],
        group: perms[1][1],
        other: perms[1][2]
    }, {
        key: 'perm3',
        name: intl.get('execute'),
        user: perms[2][0],
        group: perms[2][1],
        other: perms[2][2]
    }];

    return permData;
}

function analysisPerms(permStr) {
    let readStr = permStr.slice(1, 4);
    let writeStr = permStr.slice(4, 7);
    let executeStr = permStr.slice(7, 10);
    let strArray = [readStr, writeStr, executeStr];
    let permArray = constructPermArray(strArray);
    return permArray;
}

function constructPermArray(strArray) {
    let userArray = [];
    let groupArray = [];
    let otherArray = [];
    strArray.map((str, index) => {
        userArray[index] = judgePerm(str[0]);
        groupArray[index] = judgePerm(str[1]);
        otherArray[index] = judgePerm(str[2]);
    });
    return [userArray, groupArray, otherArray];
}

function judgePerm(word) {
    if(word !== '-') {
        return true;
    }else {
        return false;
    }
}

export function updatePermData(record, type, permData) {
    permData.map((perm) => {
        if(perm.key === record.key) {
            perm[type] = !perm[type];
        }
    });
    return permData;
}

export function updatePermMode(permData) {
    let userArray = [];
    let groupArray = [];
    let otherArray = [];
    permData.map((perm, index) => {
        userArray[index] = perm.user;
        groupArray[index] = perm.group;
        otherArray[index] = perm.other;
    });
    let permMode = '0o' + computePermNum([userArray, groupArray, otherArray]);
    return permMode;
}

function computePermNum(rolesArray) {

    let characterArray = [];
    rolesArray.map((roles, k) => {
        let num = 0;
        roles.map((role, index) => {
            if(role) {
                switch(index) {
                    case 0:
                        num += 4;
                        break;
                    case 1:
                        num += 2;
                        break;
                    case 2:
                        num += 1;
                        break;
                    default:
                        break;
                }
            }
        });
        characterArray[k] = num.toString();
    });
    return characterArray.join('');
}

export function getSelectedPath(selectedRows) {
    let rowPath = [];
    selectedRows.map((row) => {
        rowPath.push(row.path);
    });
    return rowPath;
}

export function getPopupType(name, options) {
    let popupType = '';
    options.map(option => {
        if(option.name === name) {
            popupType = option.id;
        }
    });
    return popupType;
}

export function sortHDFSFiles(files) {

    if(!files) {
        return [];
    }
    files.map((file,index) => {
        if(file.name === '.') {
            files.splice(index, 1);
            files.splice(0, 0, file)
        }
    });
    files.map((file,index) => {
        if(file.name === '..') {
            files.splice(index, 1);
            files.splice(0, 0, file)
        }
    });

    return files;
}
