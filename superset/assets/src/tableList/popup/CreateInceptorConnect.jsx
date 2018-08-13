import React from 'react';
import ReactDOM from 'react-dom';
import { render } from 'react-dom';
import { fetchInceptorConnectAdd, testConnection } from '../../databaseList/actions';
import { Select, Alert, Tooltip } from 'antd';
import PropTypes from 'prop-types';
import { getDatabaseDefaultParams } from '../../../utils/utils';

const defaultParams = getDatabaseDefaultParams();

class CreateInceptorConnect extends React.Component {
    constructor(props) {
        super(props);
        this.state = {
            exception: {},
            connected: false
        };
        // bindings
        this.confirm = this.confirm.bind(this);
        this.closeDialog = this.closeDialog.bind(this);
        this.showDialog = this.showDialog.bind(this);
        this.testConnection = this.testConnection.bind(this);
        this.closeAlert = this.closeAlert.bind(this);
    };

    showDialog() {
        this.refs.popupInceptorConnectAdd.style.display = "flex";
    }

    closeDialog() {
        ReactDOM.unmountComponentAtNode(document.getElementById("popup_root"));
    }

    testConnection() {
        const self = this;
        const { dispatch } = self.props;
        const connectObj = {
            database_name: 'none',
            sqlalchemy_uri: self.refs.connectionUri.value,
            args: self.refs.databaseArgs.value
        };
        dispatch(testConnection(connectObj, callback));

        function callback(success) {
            let exception = {};
            let connected;
            if(success) {
                exception.type = "success";
                exception.message = "该连接是一个合法连接";
                connected = true;
            }else {
                exception.type = "error";
                exception.message = "该连接是一个不合法连接";
                connected = false;
            }
            self.setState({
                exception: exception,
                connected: connected
            });
            render(
                <Alert
                    message={self.state.exception.message}
                    type={self.state.exception.type}
                    onClose={self.closeAlert('test-connect-tip')}
                    closable={true}
                    showIcon
                />,
                document.getElementById('test-connect-tip')
            );
        }
    }

    closeAlert(id) {

        ReactDOM.unmountComponentAtNode(document.getElementById(id));
    }

    confirm() {
        const self = this;
        const { dispatch } = self.props;
        const inceptorObj = {
            database_name: self.refs.connectionName.value,
            description: self.refs.connectionDes.value,
            sqlalchemy_uri: self.refs.connectionUri.value,
            args: self.refs.databaseArgs.value
        };

        dispatch(fetchInceptorConnectAdd(inceptorObj, callback));
        function callback(success, message) {
            if(success) {
                self.props.callbackRefresh('inceptor');
                ReactDOM.unmountComponentAtNode(document.getElementById("popup_root"));
            }else {
                render(
                    <Alert
                        message="Error"
                        type="error"
                        description={message.message}
                        onClose={self.closeAlert('add-connect-tip')}
                        closable={true}
                        showIcon
                    />,
                    document.getElementById('add-connect-tip')
                );
            }
        }
    }

    render() {

        return (
            <div className="popup" ref="popupInceptorConnectAdd">
                <div className="popup-dialog popup-md">
                    <div className="popup-content">
                        <div className="popup-header">
                            <div className="header-left">
                                <i className="icon icon-connect" />
                                <span>新建连接</span>
                            </div>
                            <div className="header-right">
                                <i className="icon icon-close" onClick={this.closeDialog}/>
                            </div>
                        </div>
                        <div className="popup-body">
                            <div className="dialog-item">
                                <div className="item-left">
                                    <span>连接名称：</span>
                                </div>
                                <div className="item-right">
                                    <input
                                        className="tp-input dialog-input"
                                        defaultValue=""
                                        ref="connectionName"
                                    />
                                </div>
                            </div>
                            <div className="dialog-item">
                                <div className="item-left">
                                    <span>描述：</span>
                                </div>
                                <div className="item-right">
                                    <textarea
                                        className="tp-textarea dialog-area"
                                        defaultValue=""
                                        ref="connectionDes"
                                    />
                                </div>
                            </div>
                            <div className="dialog-item">
                                <div className="item-left">
                                    <span>连接参数：</span>
                                </div>
                                <div className="item-right">
                                    <textarea
                                        className="tp-textarea dialog-area"
                                        rows="5"
                                        required="required"
                                        ref="databaseArgs"
                                        defaultValue={JSON.stringify(defaultParams, undefined, 4)}
                                    />
                                </div>
                            </div>
                            <div className="dialog-item">
                                <div className="item-left">
                                    <span>连接串：</span>
                                </div>
                                <div className="item-right">
                                    <input
                                        className="tp-input dialog-input"
                                        defaultValue=""
                                        ref="connectionUri"
                                    />
                                </div>
                            </div>
                            <div className="dialog-item" style={{ position: 'relative' }}>
                                <div className="item-left"></div>
                                <div className="item-right item-connect-test">
                                    <button className="test-connect" onClick={this.testConnection}>
                                        <i className="icon icon-connect-test" />
                                        <span>测试连接</span>
                                    </button>
                                    <div id="test-connect-tip"></div>
                                </div>
                                <Tooltip title="structure your URL" placement="bottom">
                                    <i className="icon icon-info" style={{ position: 'relative', top: '-43px', left: '5px' }} />
                                </Tooltip>
                            </div>
                            <div className="error" id="add-connect-tip"></div>
                        </div>
                        <div className="popup-footer">
                            <button
                                className="tp-btn tp-btn-middle tp-btn-primary"
                                onClick={this.confirm}
                                disabled={!this.state.connected}
                            >确定</button>
                        </div>
                    </div>
                </div>
            </div>
        );
    }
}

const propTypes = {};
const defaultProps = {};

CreateInceptorConnect.propTypes = propTypes;
CreateInceptorConnect.defaultProps = defaultProps;

export default CreateInceptorConnect;
