import React from 'react';
import { render, unmountComponentAtNode } from 'react-dom';
import { Tooltip, Alert, Table } from 'antd';
import { bindActionCreators } from 'redux'
import { connect } from 'react-redux';

import { Select, TreeSelect } from './';
import * as ActionCreators from '../actions'
import { CONSTANT } from '../actions'
import { getPermColumns, updatePermData, updatePermMode } from '../module'
import { renderAlertTip } from './../../utils/utils';

import { WarningAlert } from '../../common/components/WarningAlert';

import PropTypes from 'prop-types';
import intl from "react-intl-universal";

const $ = window.$ = require('jquery');

class Popup extends React.Component {
    constructor(props, context) {
        super(props);

        this.closeDialog = this.closeDialog.bind(this);
        this.submit = this.submit.bind(this);
        this.checkIfSubmit = this.checkIfSubmit.bind(this);
        this.onInputChange = this.onInputChange.bind(this);
        this.handleFile = this.handleFile.bind(this);
        this.onPermChange = this.onPermChange.bind(this);
        this.onRecursivePermChange = this.onRecursivePermChange.bind(this);

        this.state = {}
        this.timer = 0;
    }

    componentDidUpdate() {
        this.checkIfSubmit();
        const popupType = this.props.popupNormalParam.popupType;
        this.addInputFocus(popupType);
    }

    addInputFocus(popupType) {
        if(popupType === CONSTANT.mkdir) {
            document.getElementById('folder-input-id').focus();
        }else if(popupType === CONSTANT.touch) {
            document.getElementById('file-input-id').focus();
        }
    }

    closeDialog() {
        const popupNormalParam = this.props;
        this.props.setPopupNormalParams({
            ...popupNormalParam,
            status: 'none',
            alertStatus: 'none',
            alertMsg: '',
            alertType: ''
        });
    }

    removeAlert() {
        this.props.setPopupNormalParam({
            alertStatus: 'none'
        });
    }

    checkIfSubmit() {
        var fields = $(".popup-body input[required]");
        let disabled = null;

        fields.each((idx, obj) => {
            if (obj.value === '') {
                disabled = 'disabled';
                return;
            }
        });

        if (disabled === this.props.popupNormalParam.disabled) {
            return;
        }
        this.props.setPopupNormalParam({
            disabled: disabled
        });
    }

    onInputChange(e, eventType) {
        const target = e.currentTarget;
        const key = target.name;
        let val = '';
        if (eventType === "selectFile") {
            const pathArray = target.value.split('\\');
            val = pathArray[pathArray.length - 1];
        } else {
            val = target.value;
        }
        const setPopupNormalParam = this.props.setPopupNormalParam;
        setPopupNormalParam({
            [key]: val
        });

        this.removeAlert();

        clearTimeout(this.timer);
        this.timer = setTimeout(() => {
            this.checkIfSubmit();
        }, 800);
    }

    handleFile(e) {
        const me = this;
        let files = this.refs.fileSelect.files;
        let length = files.length;
        let names = '';
        let reader;
        for(let i=length-1; i>=0; i--) {
            reader = new FileReader();
            names = files[i].name + ' \n' + names;
            reader.readAsArrayBuffer(files[0]);
/*            reader.onload = function(event) {
                event.currentTarget.name = 'binaryFile';
                event.currentTarget.value = event.target.result;
                me.onInputChange(event);
            }*/
        }
        this.refs.fileName.innerText = names;
        // this.onInputChange(e, 'selectFile');
    }

    onPermChange(record, type) {
        const {setPermData, setPermMode, popupNormalParam} = this.props;
        const permData = updatePermData(record, type, popupNormalParam.permData);
        const permMode = updatePermMode(permData);
        setPermData(permData);
        setPermMode(permMode);
    }

    onRecursivePermChange() {
        const {setRecursivePerm} = this.props;
        setRecursivePerm(this.refs.recursivePermRef.checked);
    }

    submit() {
        const popupNormalParam = this.props.popupNormalParam;
        popupNormalParam.submit();

        popupNormalParam.popupType === CONSTANT.download && this.closeDialog();
    }

    render() {
        const me = this;

        const {popupNormalParam, setPopupNormalParams, fetchHDFSList} = this.props;
        const {popupType, disabled, treeData, treeVal, status, alertStatus, alertMsg,
            alertType, permData, deleteTips, dir_name, dest_path, filename, file_name} = popupNormalParam;
        let btnTitle = intl.get('submit');
        if (popupType === CONSTANT.noSelect) {
            btnTitle =  intl.get('confirm');
        }

        let permColumns = [];

        //TODO:popupType from props
        const getConfig = (popupType) => {
            let title = '';
            switch (popupType) {
                case CONSTANT.move:
                    title = intl.get('move_to');
                    break;
                case CONSTANT.copy:
                    title = intl.get('copy_to');
                    break;
                case CONSTANT.download:
                    title = intl.get('download');
                    break;
                case CONSTANT.auth:
                    title = intl.get('change_auth');
                    permColumns = getPermColumns(me);
                    break;
                case CONSTANT.upload:
                    title = intl.get('upload_file');
                    break;
                case CONSTANT.mkdir:
                    title = intl.get('create_folder');
                    break;
                case CONSTANT.touch:
                    title = intl.get('create_file');
                    break;
                case CONSTANT.remove:
                    title = intl.get('delete');
                    break;
                case CONSTANT.noSelect:
                    title = intl.get('tips');
            }
            return {
                title
            };
        }

        const config = getConfig(popupType);

        const origin = window.location.origin;
        const actionURL = `${origin}/hdfs/upload/?dest_path=${dest_path}`;

        const getChildren = (popupType) => {
            switch (popupType) {
                case CONSTANT.move:
                    return <div
                        className="popup-body move"
                        style={{
                            height: '200px'
                        }}
                    >
                        <div id="hdfs-tree-select" className="hdfs-tree-select"></div>
                        <div className="dialog-item">
                            <div className="item-left">
                                <span>{intl.get('move_into')}</span>
                            </div>
                            <div className="item-right">
                                <div id="edit_pop_select">
                                    <TreeSelect
                                        treeData={treeData}
                                        treeVal={treeVal}
                                        fetchHDFSList={fetchHDFSList}
                                        setPopupNormalParams={setPopupNormalParams}
                                    />
                                </div>
                            </div>
                        </div>
                    </div>
                    break;
                case CONSTANT.copy:
                    return <div
                        className="popup-body move"
                        style={{
                        height: '200px'
                        }}
                    >
                        <div id="hdfs-tree-select" className="hdfs-tree-select"></div>
                        <div className="dialog-item">
                            <div className="item-left">
                                <span>{intl.get('copy_into')}</span>
                            </div>
                            <div className="item-right">
                                <div id="edit_pop_select">
                                    <TreeSelect
                                        treeVal={treeVal}
                                        treeData={treeData}
                                        fetchHDFSList={fetchHDFSList}
                                        setPopupNormalParams={setPopupNormalParams}
                                    />
                                </div>
                            </div>
                        </div>
                    </div>
                    break;
                case CONSTANT.download:
                    return <div
                        className="popup-body move"
                        style={{
                        height: '200px'
                        }}
                    >
                        <div id="hdfs-tree-select" className="hdfs-tree-select"></div>
                        <div className="dialog-item">
                            <div className="item-left">
                                <span></span>
                            </div>
                            <div className="item-right">
                                {intl.get('download_tip')} ?
                            </div>
                        </div>
                    </div>
                    break;
                case CONSTANT.auth:
                    return <div className="popup-body" style={{paddingLeft: 20, paddingRight: 20}}>
                        <Table
                            columns={permColumns}
                            dataSource={permData}
                            size='small'
                            pagination={false}
                        />
                        <div className="recursive-perm">
                            <div>
                                <div className="col-20 perm-name">{intl.get('recursion')}</div>
                                <div className="col-55"></div>
                                <div className="col-25 perm-value">
                                    <input
                                        type="checkbox"
                                        onClick={this.onRecursivePermChange}
                                        ref="recursivePermRef"
                                    />
                                </div>
                            </div>
                        </div>
                    </div>
                    break;
                case CONSTANT.upload:
                    return <div className="popup-body">
                        <div className="dialog-item">
                            <div className="item-left">
                                <span>{intl.get('upload_to')}</span>
                            </div>
                            <div className="item-right">
                                <input
                                    type="text"
                                    value={dest_path}
                                    required="required"
                                    className="tp-input dialog-input"
                                    disabled
                                />
                            </div>
                        </div>
                        <div className="dialog-item">
                            <div className="item-left">
                                <span></span>
                            </div>

                            <div className="item-right">
                            <form action={actionURL} method="post" encType="multipart/form-data">
                                <label className="file-browser" htmlFor="xFile" style={{width: 200, color: '#fff'}}>
                                    {intl.get('select_file')}
                                </label>
                                <div className="file-name">
                                    <i className="icon icon-file"/>
                                    <span className="file-names" ref="fileName">{file_name}</span>
                                    <input type="hidden" required="required" value={file_name}/>
                                </div>
                                <div className="file-upload" style={{display: 'none'}}>
                                  <input 
                                    multiple="multiple" 
                                    className="file-select" 
                                    required="required" 
                                    type="file" id="xFile" 
                                    onChange={this.handleFile} 
                                    name="list_file" 
                                    ref="fileSelect" /> 
                                </div>
                                <input type="submit" value={btnTitle} className="tp-btn tp-btn-middle tp-btn-primary" />
                            </form>
                            </div>
                        </div>
                    </div>;
                    break;
                case CONSTANT.mkdir:
                    return <div className="popup-body">
                                <div className="dialog-item">
                                    <div className="item-left">
                                        <span>{intl.get('folder_name')}</span>
                                    </div>
                                    <div className="item-right">
                                        <input
                                            required="required"
                                            type="text"
                                            value={dir_name}
                                            id="folder-input-id"
                                            name="dir_name"
                                            onChange={this.onInputChange}
                                            className="tp-input dialog-input"
                                        />
                                    </div>
                                </div>
                            </div>;
                    break;
                case CONSTANT.touch:
                    return <div className="popup-body">
                                <div className="dialog-item">
                                    <div className="item-left">
                                        <span>{intl.get('file_name')}</span>
                                    </div>
                                    <div className="item-right">
                                        <input
                                            required="required"
                                            type="text"
                                            value={filename}
                                            id="file-input-id"
                                            name="filename"
                                            onChange={this.onInputChange}
                                            className="tp-input dialog-input"
                                        />
                                    </div>
                                </div>
                            </div>;
                    break;
                case CONSTANT.remove:
                    return <div className="popup-body">
                        <WarningAlert
                            message={deleteTips}
                        />
                    </div>;

                case CONSTANT.noSelect:
                    return <div className="popup-body">
                        <div className="warning">
                            <Alert
                                message="Warning"
                                description={intl.get('tip:not_selected_HDFS')}
                                type="warning"
                                showIcon
                            />
                        </div>
                    </div>;
                default:
                    return '';
            }
        }

        if(popupType === CONSTANT.upload) {
            return (
                <div className="popup" ref="popupContainer" style={{
                    display: status
                }}>
                    <div className="popup-dialog popup-md">
                        <div className="popup-content">
                            <div className="popup-header">
                                <div className="header-left">
                                    <i className={'icon '}/>
                                    <span>{config.title}</span>
                                </div>
                                <div className="header-right">
                                    <i className="icon icon-close" onClick={this.closeDialog}/>
                                </div>
                            </div>
                            {getChildren(popupType)}
                            <div className={alertStatus + ' alert-wrapper'}>
                                <Alert message={alertMsg} type={alertType} closable={true} showIcon />
                            </div>
                        </div>
                    </div>
                </div>
            );
        } else {
            return (
                <div className="popup" ref="popupContainer" style={{
                    display: status
                }}>
                    <div className="popup-dialog popup-md">
                        <div className="popup-content">
                            <div className="popup-header">
                                <div className="header-left">
                                    <i className={'icon '}/>
                                    <span>{config.title}</span>
                                </div>
                                <div className="header-right">
                                    <i className="icon icon-close" onClick={this.closeDialog}/>
                                </div>
                            </div>
                            {getChildren(popupType)}
                            <div className={alertStatus + ' alert-wrapper'}>
                                <Alert message={alertMsg} type={alertType} closable={true} showIcon />
                            </div>
                            <div className="popup-footer">
                                <button
                                    disabled={disabled}
                                    className="tp-btn tp-btn-middle tp-btn-primary"
                                    onClick={me.submit}>
                                    {btnTitle}
                                </button>
                            </div>
                        </div>
                    </div>
                </div>
            );
        }

    }
}

const mapStateToProps = function(state) {
    const {popupNormalParam, condition} = state;
    return {
        popupNormalParam,
        condition
    }
}

Popup.propTypes = {};
Popup.defaultProps = {};

export default connect(mapStateToProps, ActionCreators)(Popup);