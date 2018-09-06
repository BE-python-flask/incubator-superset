/**
 * Created by haitao on 17-5-11.
 */
import React from 'react';
import ReactDOM from 'react-dom';
import { Select, Alert, Tooltip } from 'antd';
import intl from 'react-intl-universal';
import PropTypes from 'prop-types';

import NestedTable from '../components/NestedTable';
import { 
    fetchDashboardImport, 
    setBinaryFile, 
    fetchBeforeImport,
    fetchPosts } from '../actions';
import { renderAlertErrorInfo } from '../../utils/utils';

class ImportDashboard extends React.Component {
    constructor(props, context, updater) {
        super(props);
        console.log(props, context, updater);

        this.state = {
            enableConfirm: false,
            duplicatedList: null
        };
        // bindings
        this.confirm = this.confirm.bind(this);
        this.closeAlert = this.closeAlert.bind(this);

        this.handleFile = this.handleFile.bind(this);
    };

    closeAlert(id) {
        ReactDOM.unmountComponentAtNode(document.getElementById(id));
    }

    handleTitleChange(e) {
        this.props.dashboard.name = e.currentTarget.value;
        let enableConfirm;
        if(!e.currentTarget.value || e.currentTarget.value.length === 0) {
            enableConfirm = false;
        }else {
            enableConfirm = true;
        }
        this.setState({
            dashboard: this.props.dashboard,
            enableConfirm: enableConfirm
        });
        this.closeAlert('add-dashboard-error-tip');
    }

    handleFile(e) {
        const me = this;
        const { dispatch } = me.props;
        let file = this.refs.fileSelect.files[0];
        let name = file.name;
        this.refs.fileName.value = name;

        let reader = new FileReader();
        reader.readAsArrayBuffer(file);
        reader.onload = function(event) {
            event.currentTarget.name = 'binaryFile';
            event.currentTarget.value = event.target.result;
            dispatch(setBinaryFile(event.target.result));
            dispatch(fetchBeforeImport((status, data) => {
                if (status) {
                    me.setState({
                        enableConfirm: true,
                        duplicatedList: data
                    });
                    me.closeAlert('add-dashboard-error-tip');
                } else {
                    renderAlertErrorInfo(data, 'add-dashboard-error-tip', '100%', me);
                    me.setState({
                        enableConfirm: false,
                        duplicatedList: []
                    });
                }
            }));
        }
    }

    confirm() {
        const me = this;
        const { dispatch } = me.props;
        dispatch(fetchDashboardImport((success, message) => {
            if(success) {
                me.closeAlert("popup_root");
                dispatch(fetchPosts());
            }else {
                renderAlertErrorInfo(message, 'add-dashboard-error-tip', '100%', me);
            }
        }));
    }

    render() {
        const me = this;
        const Option = Select.Option;
        const duplicatedList = this.state.duplicatedList;

        return (
            <div className="popup">
                <div className="popup-dialog popup-md">
                    <div className="popup-content">
                        <div className="popup-header">
                            <div className="header-left">
                                <i className="icon icon-import" />
                                <span>{intl.get('DASHBOARD.IMPORT')}</span>
                            </div>
                            <div className="header-right">
                                <i
                                    className="icon icon-close"
                                    onClick={argus => this.closeAlert('popup_root')}
                                />
                            </div>
                        </div>
                        <div className="popup-body">
                            <div className="dialog-item">
                                <div className="item-left">
                                    <span>{intl.get('DASHBOARD.SELECT_FILE')}：</span>
                                </div>
                                <div className="item-right">
                                    <input 
                                        className="tp-input dialog-input"
                                        type="text"  
                                        ref="fileName"
                                    />
                                    <label 
                                        className="file-browser" 
                                        htmlFor="xFile" 
                                        style={{
                                            position: 'absolute', 
                                            left: 0, top: 0,
                                            width: '100%', height: '100%',
                                            background: 'transparent'
                                        }}
                                    >
                                    </label>
                                    <input 
                                        style={{display: 'none'}}
                                        multiple="multiple" 
                                        className="file-select" 
                                        required="required" 
                                        type="file" id="xFile" 
                                        onChange={this.handleFile} 
                                        name="list_file" 
                                        ref="fileSelect" /> 
                                </div>
                            </div>
                            <div>
                                { 
                                    duplicatedList && 
                                    <NestedTable 
                                        dispatch={this.props.dispatch}
                                        duplicatedList={duplicatedList}
                                    />
                                }
                            </div>
                        </div>
                        <div className="error" id="add-dashboard-error-tip"></div>
                        <div className="popup-footer">
                            <button
                                className="tp-btn tp-btn-middle tp-btn-primary"
                                onClick={this.confirm}
                                disabled={!this.state.enableConfirm}
                            >{intl.get('DASHBOARD.IMPORT')}</button>
                        </div>
                    </div>
                </div>
            </div>
        );
    }
}

const propTypes = {};
const defaultProps = {};

ImportDashboard.propTypes = propTypes;
ImportDashboard.defaultProps = defaultProps;

export default ImportDashboard;