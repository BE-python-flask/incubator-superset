import React from 'react';
import ReactDOM, {render} from 'react-dom';
import PropTypes from 'prop-types';
import {Table, Select, Tooltip} from 'antd';

import * as perm from '../../action/permAction';
import intl from 'react-intl-universal';
import * as utils from '../../../utils/utils';
import * as model from './model';
import {PermInfo} from './PermInfo';

const rootMountId = 'popup_root';
const alertMountId = 'grant-perm-error-tip';
const checkboxMountId = 'grant-perm-checkbox-container';
const grantInfoMountId = 'grant-info-mount-container';

const PREFIX = 'GRANT_PERM_';

class PermPopup extends React.Component {
    constructor(props) {
        super(props);
        this.state = {
            initDone: false,
            grantInfo: '',
            selectOptions: [],
            tableColumns: [],
            tableDataSource: [],
            permCheckboxes: [],
            selectedUser: '',
            grantedActions: []
        };
        this.grantPerm = this.grantPerm.bind(this);
        this.closeAlert = this.closeAlert.bind(this);
        this.handleChange = this.handleChange.bind(this);
        this.onSelectChange = this.onSelectChange.bind(this);
        this.revokePerm = this.revokePerm.bind(this);
        this.switchGrantInfo = this.switchGrantInfo.bind(this);
    };

    closeAlert(id) {
        ReactDOM.unmountComponentAtNode(document.getElementById(id));
    }

    grantPerm() {
        const {objectType, objectName} = this.props;
        const {selectedUser, grantedActions} = this.state;
        const self = this;
        perm.grantPermission({
            username: selectedUser,
            object_type: objectType,
            object_name: objectName,
            actions: grantedActions
        }, callback);
        function callback(success, data) {
            if(success) {
                self.getPermTypes();
                self.searchPermissions();
                const response = {
                    type: 'success',
                    message: intl.get('POPUP.CONFIG_SUCCESS')
                };
                utils.renderAlertTip(response, alertMountId, '100%');
            }else {
                utils.renderAlertErrorInfo(data, alertMountId, '100%', self);
            }
        }
    }

    handleChange(e) {
        let grantedPerms = this.state.grantedActions.slice(0);
        const checkbox = e.target;
        const name = checkbox.name;
        const checked = checkbox.checked;
        const index = grantedPerms.indexOf(name);
        if(checked && index === -1) {
            grantedPerms.push(name);
            if(name === 'EDIT') {
                this.setCheckboxState(PREFIX + 'READ', 'READ', grantedPerms);
            }else if(name === 'ADMIN') {
                this.setCheckboxState(PREFIX + 'READ', 'READ', grantedPerms);
                this.setCheckboxState(PREFIX + 'EDIT', 'EDIT', grantedPerms);
            }
        }else if(!checked && index > -1) {
            grantedPerms.splice(index, 1);
            if(name === 'READ') {
                this.cancelCheckboxState(PREFIX + 'EDIT', 'EDIT', grantedPerms);
                this.cancelCheckboxState(PREFIX + 'ADMIN', 'ADMIN', grantedPerms);
            }else if(name === 'EDIT') {
                this.cancelCheckboxState(PREFIX + 'ADMIN', 'ADMIN', grantedPerms);
            }
        }
        this.setState({
            grantedActions: grantedPerms
        });
    }

    setCheckboxState(id, name, perms) {
        const checkboxEl = document.getElementById(id);
        if(checkboxEl && !checkboxEl.checked) {
            checkboxEl.checked = true;
            perms.push(name);
        }
    }

    cancelCheckboxState(id, name, perms) {
        const checkboxEl = document.getElementById(id);
        const index = perms.indexOf(name);
        if(checkboxEl && checkboxEl.checked && index > -1) {
            checkboxEl.checked = false;
            perms.splice(index, 1);
        }
    }

    onSelectChange(value) {
        this.getPermTypes();
        this.setState({
            selectedUser: value
        });
    }

    revokePerm(record) {
        const self = this;
        const {objectType, objectName} = this.props;
        perm.revokePermission({
            username: record.name,
            object_type: objectType,
            object_name: objectName,
            actions: record.perm.split(', ')
        }, callback);
        function callback(success, data) {
            if(success) {
                self.searchPermissions();
            }else {
                utils.renderAlertErrorInfo(data, alertMountId, '100%', self);
            }
        }
    }

    getGuardianUsers() {
        const self = this;
        perm.getGuardianUsers(callback);
        function callback(success, data) {
            if(success) {
                self.setState({
                    selectOptions: model.makeSelectOptions(data.usernames)
                });
            }else {
                utils.renderGlobalErrorMsg(data);
            }
        }
    }

    getPermTypes() {
        const self = this;
        perm.getPermTypes(callback);
        function callback(success, data) {
            if(success) {
                self.clearCheckboxState(data.permissions);
                self.setState({
                    grantedActions: [],
                    permCheckboxes: model.makePermCheckboxes(data.permissions, self, PREFIX)
                });
            }else {
                utils.renderGlobalErrorMsg(data);
            }
        }
    }

    clearCheckboxState(checkboxNames) {
        const els = document.getElementsByTagName('input');
        for(let i=0; i < els.length; i++) {
            if(els[i].type === "checkbox" && checkboxNames.indexOf(els[i].name) > -1) {
                els[i].checked = false;
            }
        }
    }

    searchPermissions() {
        const self = this;
        const {objectType, objectName} = this.props;
        perm.searchPermissions({
            object_type: objectType,
            object_name: objectName
        }, callback);
        function callback(success, data) {
            if(success) {
                const tbDataSource = model.makeTableDataSource(data);
                const tbColumns = model.makeTableColumns(self, intl);
                self.setState({
                    tableDataSource: tbDataSource,
                    tableColumns: tbColumns
                });
            }else {
                utils.renderGlobalErrorMsg(data);
            }
        }
    }

    loadIntlResource() {
        const callback = () => {
            this.searchPermissions();
            this.setState({ initDone: true });
        };
        utils.loadIntlResources(callback, 'popup');
    }

    getPermInfo() {
        const { objectType, objectId } = this.props;
        const callback = (success, data) => {
            if(success) {
                this.setState({
                    grantInfoShow: true,
                    grantInfo: data.split('\n')
                });
                this.showGrantInfo(grantInfoMountId);
            }else {
                utils.renderGlobalErrorMsg(data);
            }
        };

        /* backend api not unified, add judge conditon */
        perm.getPermInfo({
            type: objectType === utils.OBJECT_TYPE.DATASET ? utils.OBJECT_TYPE.TABLE : objectType,
            id: objectId
        }, callback);
    }

    switchGrantInfo(mountId) {
        const showed = this.state.grantInfoShow;
        this.setState({
            grantInfoShow: !showed
        });
        if(!showed) {
            this.showGrantInfo(mountId);
        }else {
            this.hideGrantInfo(mountId);
        }

    }

    showGrantInfo(mountId) {
        render(
            <PermInfo
                infos={this.state.grantInfo}
                key={+new Date()}
            />,
            document.getElementById(mountId)
        );
    }

    hideGrantInfo(mountId) {
        ReactDOM.unmountComponentAtNode(document.getElementById(mountId));
    }

    componentDidMount() {
        this.getGuardianUsers();
        this.getPermTypes();
        this.searchPermissions();
        this.getPermInfo();
        this.loadIntlResource();
    }

    render() {
        const {
            selectOptions,
            tableColumns,
            tableDataSource,
            permCheckboxes,
            selectedUser,
            grantInfoShow,
            grantedActions
        } = this.state;
        return (this.state.initDone &&
            <div className="popup">
                <div className="popup-dialog popup-md">
                    <div className="popup-content">
                        <div className="popup-header">
                            <div className="header-left">
                                <i className="icon icon-dashboard-popup" />
                                <span>{intl.get('POPUP.PERM_GRANT')}</span>
                            </div>
                            <div className="header-right">
                                <i
                                    className="icon icon-close"
                                    onClick={args => this.closeAlert(rootMountId)}
                                />
                            </div>
                        </div>
                        <div className="popup-body">
                            <div className="dialog-item">
                                <div className="item-left" style={{width: 80, 'paddingLeft': 20}}>
                                    <span>{intl.get('POPUP.USER_NAME')}</span>
                                </div>
                                <div className="item-right" style={{width: 480}}>
                                    <Select
                                        style={{width: '100%'}}
                                        placeholder={intl.get('POPUP.PLEASE_SELECT')}
                                        onChange={this.onSelectChange}
                                    >
                                        {selectOptions}
                                    </Select>
                                </div>
                            </div>
                            <div className="dialog-item">
                                <div className="item-left" style={{width: 80, 'paddingLeft': 20}}>
                                    <span>{intl.get('POPUP.PERM')}</span>
                                </div>
                                <div
                                    id={checkboxMountId}
                                    style={{display: 'flex', justifyContent: 'space-between', width: 400}}>
                                    {permCheckboxes}
                                </div>
                            </div>
                            <div
                                className="dialog-item"
                                style={{display: 'flex',justifyContent: 'center', margin: '20 auto 10 auto'}}>
                                <button
                                    className="tp-btn tp-btn-middle tp-btn-primary"
                                    onClick={this.grantPerm}
                                    disabled={!selectedUser || grantedActions.length === 0}
                                >{intl.get('POPUP.CONFIG_PERM')}</button>
                                <Tooltip
                                    placement="top"
                                    title={grantInfoShow ? intl.get('POPUP.CLICK_HIDE_GRANT_INFO') : intl.get('POPUP.CLICK_SHOW_GRANT_INFO')}
                                >
                                    <i
                                        style={{marginLeft: 10}}
                                        className="icon icon-info"
                                        onClick={argus => this.switchGrantInfo(grantInfoMountId)}
                                    />
                                </Tooltip>
                            </div>
                            <div id={grantInfoMountId} className="dialog-item"></div>
                            <div className="table-grant-perm" style={{margin: '10 20'}}>
                                <Table
                                    dataSource={tableDataSource}
                                    columns={tableColumns}
                                    pagination={false}
                                    size="small"
                                />
                            </div>
                        </div>
                        <div className="error" id={alertMountId}></div>
                    </div>
                </div>
            </div>
        );
    }
}

export default PermPopup;