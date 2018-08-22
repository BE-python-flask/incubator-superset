import React from 'react';
import ReactDOM, {render, unmountComponentAtNode} from 'react-dom';
import {Tooltip, Alert} from 'antd';

import {Select} from '../components';
import PropTypes from 'prop-types';
import {connectionTypes} from '../actions';
import * as actions from '../actions';
import * as utils from '../../utils/utils';
import intl from 'react-intl-universal';
import {isCorrectConnection, argsValidate, connectDefaultInfo} from '../utils';

class ConnectionAdd extends React.Component {
    constructor (props, context) {
        super(props);
        this.state = {
            exception: {},
            database: {
                database_name: '',
                database_type: '',
                description: '',
                sqlalchemy_uri: connectDefaultInfo[props.connectionType].str.defaultValue,
                args: JSON.stringify(
                    connectDefaultInfo[props.connectionType].args.defaultValue,
                    undefined,
                    4
                ),
                connection_name: '',
                httpfs: '',
                // database_id:''
            },
            connectionNames: [],
            connected: false,
            disabled: true

        };

        this.submit = this.submit.bind(this);
        this.closeAlert = this.closeAlert.bind(this);
        // this.setSelectConnection = this.setSelectConnection.bind(this);
        this.handleChange = this.handleChange.bind(this);
        this.argsOnBlur = this.argsOnBlur.bind(this);
    }

    componentDidMount () {
        const { connectionType } = this.props;
        if(connectionType === connectionTypes.hdfs) {
            this.fetchConnectionNames();
        }
    }

    testConnection(testCallBack) {
        const self = this;
        const { dispatch, connectionType } = this.props;
        if(isCorrectConnection(connectionType, connectionTypes)) {
            if(!argsValidate(this.state.database.args)) {
                utils.renderAlertErrorInfo(intl.get('DATABASE.CONN_GRAMMAR_ERROR'), 'add-connect-error-tip', '100%', this);
                return;
            }
            dispatch(actions.testConnection({
                database_name: this.state.database.database_name,
                sqlalchemy_uri: this.state.database.sqlalchemy_uri,
                args: this.state.database.args
            }, callback));
        }else if(connectionType === connectionTypes.hdfs) {
            dispatch(actions.testHDFSConnection(this.state.database.httpfs, callback));
        }
        function callback(success, message) {
            let exception = {};
            let connected;
            if(success) {
                exception.type = "success";
                exception.message = intl.get('DATABASE.CONN_IS_ILLEGAL');
                connected = true;
            }else {
                exception.type = "error";
                exception.message = intl.get('DATABASE.CONN_IS_UNILLEGAL');
                connected = false;
                utils.renderAlertErrorInfo(message, 'add-connect-error-tip', '100%', self);
            }
            self.setState({
                connected: connected
            });
            self.formValidate(self.state.database);
            utils.renderAlertTip(exception, 'test-add-connect-tip', '100%');
        }
        if(typeof testCallBack === 'function') {
            testCallBack(self.state.connected);
        }
    }

    closeAlert(id) {
        ReactDOM.unmountComponentAtNode(document.getElementById(id));
    }

/*    setSelectConnection(databaseId) {
        const database = {
            ...this.state.database,
            database_id: databaseId
        };
        this.setState({
            database: database
        });
        this.formValidate(database);
    }*/

    handleChange(e) {
        const target = e.currentTarget;
        const name = target.name;
        const val = target.value;
        const database = {...this.state.database, [name]: val};
        this.setState({
            database: database
        });
        this.formValidate(database);
    }

    formValidate(database) {
        let disabled;
        const { connectionType } = this.props;
        if(isCorrectConnection(connectionType, connectionTypes)) {
            if((database.database_name && database.database_name.length > 0) &&
                (database.sqlalchemy_uri && database.sqlalchemy_uri.length > 0) &&
                argsValidate(database.args) && this.state.connected) {
                disabled = false;
            }else {
                disabled = true;
            }
        }else if(this.props.connectionType === connectionTypes.hdfs) {
            if((database.connection_name && database.connection_name.length > 0) &&
                (database.httpfs && database.httpfs.length > 0) && this.state.connected) {
                disabled = false;
            }else {
                disabled = true;
            }
        }
        this.setState({
            disabled: disabled
        });
        this.closeAlert('add-connect-error-tip');
    }

    argsOnBlur() {
        const args = this.state.database.args;
        if(!argsValidate(args)) {
            utils.renderAlertErrorInfo(
                intl.get('DATABASE.CONN_GRAMMAR_ERROR'),
                'add-connect-error-tip',
                '100%',
                this
            );
        }
    }

    addConnection() {
        const { connectionType, dispatch } = this.props;
        const self = this;
        function callback(success, message) {
            if(success) {
                self.closeAlert('popup_root');
            }else {
                utils.renderAlertErrorInfo(message, 'add-connect-error-tip', '100%', self);
            }
        }
        if(isCorrectConnection(connectionType, connectionTypes)) {
            dispatch(actions.fetchInceptorConnectAdd({
                database_name: this.state.database.database_name,
                database_type: connectionType,
                description: this.state.database.description,
                sqlalchemy_uri: this.state.database.sqlalchemy_uri,
                args: this.state.database.args
            }, callback) );
        }else if(connectionType === connectionTypes.hdfs) {
            dispatch(actions.fetchHDFSConnectAdd({
                connection_name: this.state.database.connection_name,
                description: this.state.database.description,
                httpfs: this.state.database.httpfs,
                // database_id: this.state.database.database_id
            }, callback))
        }
    }

    submit () {
        const self = this;
        if(this.state.connected) {
            this.addConnection();
        }else {
            this.testConnection(testCallBack);
            function testCallBack(success, data) {
                if(success) {
                    self.addConnection();
                }else {
                    utils.renderGlobalErrorMsg(data);
                }
            }
        }
    }

    fetchConnectionNames () {
        const self = this;
        const { dispatch } = this.props;
        const callback = (success, data) => {
            if(success) {
                const connectionNames = [];
                data.data.map((obj, key) => {
                    connectionNames.push({
                        id:obj.id,
                        label:obj.database_name
                    })
                });
                self.setState({connectionNames:connectionNames});
            }else {
                utils.renderGlobalErrorMsg(data);
            }

        };
        dispatch(actions.fetchConnectionNames(callback));
    }

    render () {
        const self = this;
        const {connectionType} = this.props;

        return (
            <div className="popup">
                <div className="popup-dialog popup-md">
                    <div className="popup-content">
                        <div className="popup-header">
                            <div className="header-left">
                                <i className='icon icon-connect'/>
                                <span>{intl.get('DATABASE.ADD') + ' '}{connectionType}{' ' + intl.get('DATABASE.CONNECTION')}</span>
                            </div>
                            <div className="header-right">
                                <i
                                    className="icon icon-close"
                                    onClick={argus => this.closeAlert('popup_root')}
                                />
                            </div>
                        </div>
                        <div className="popup-body">
                            <div style={{ display: isCorrectConnection(connectionType, connectionTypes)?'block':'none' }} >
                                <div className="dialog-item">
                                    <div className="item-left">
                                        <i>*</i>
                                        <span>{intl.get('DATABASE.CONN_NAME')}：</span>
                                    </div>
                                    <div className="item-right">
                                        <input
                                            required="required"
                                            name="database_name"
                                            className="tp-input dialog-input"
                                            onChange={this.handleChange}
                                            autoFocus
                                        />
                                    </div>
                                </div>
                                <div className="dialog-item">
                                    <div className="item-left">
                                        <span>{intl.get('DATABASE.DESCRIPTION')}：</span>
                                    </div>
                                    <div className="item-right">
                                        <textarea
                                            name="description"
                                            className="tp-textarea dialog-area"
                                            onChange={this.handleChange}
                                        />
                                    </div>
                                </div>

                                <div className="dialog-item">
                                    <div className="item-left">
                                        <i>*</i>
                                        <span>{intl.get('DATABASE.CONN_URI')}：</span>
                                    </div>
                                    <div className="item-right">
                                        <input
                                            defaultValue={this.state.database.sqlalchemy_uri}
                                            required="required"
                                            name="sqlalchemy_uri"
                                            className="tp-input dialog-input"
                                            onChange={this.handleChange}
                                        />
                                        <Tooltip
                                            placement="topRight"
                                            title={intl.get('DATABASE.'+ connectDefaultInfo[connectionType].str.tip)}
                                        >
                                            <i className="icon icon-infor after-icon"/>
                                        </Tooltip>
                                    </div>
                                </div>

                                <div className="dialog-item">
                                    <div className="item-left">
                                        <i>*</i>
                                        <span>{intl.get('DATABASE.CONN_PARAMS')}：</span>
                                    </div>
                                    <div className="item-right">
                                        <textarea
                                            rows="5"
                                            style={{width:'420px', height:'120px'}}
                                            required="required"
                                            name="args"
                                            defaultValue={this.state.database.args}
                                            className="tp-textarea dialog-area"
                                            onChange={this.handleChange}
                                            onBlur={this.argsOnBlur}
                                            disabled={connectionType===connectionTypes.inceptor?false:true}
                                        >
                                        </textarea>
                                        <Tooltip
                                            placement="topRight"
                                            title={intl.get('DATABASE.'+ connectDefaultInfo[connectionType].args.tip)}
                                        >
                                            <i
                                                className="icon icon-infor after-textarea-icon"
                                                style={{top: 50}}
                                            />
                                        </Tooltip>
                                    </div>
                                </div>
                            </div>
                            <div style={{ display: connectionType === connectionTypes.hdfs?'block':'none' }} >
                                <div className="dialog-item">
                                    <div className="item-left">
                                        <i>*</i>
                                        <span>{intl.get('DATABASE.CONN_NAME')}：</span>
                                    </div>
                                    <div className="item-right">
                                        <input
                                            defaultValue=''
                                            required="required"
                                            name="connection_name"
                                            className="tp-input dialog-input"
                                            onChange={this.handleChange}
                                            autoFocus
                                        />
                                    </div>
                                </div>
                                <div className="dialog-item">
                                    <div className="item-left">
                                        <span>{intl.get('DATABASE.DESCRIPTION')}：</span>
                                    </div>
                                    <div className="item-right">
                                        <textarea
                                            rows="5"
                                            defaultValue=''
                                            style={{width:'420px'}}
                                            required="required"
                                            name="description"
                                            className="tp-textarea dialog-area"
                                            onChange={this.handleChange}
                                        />
                                    </div>
                                </div>
                                <div className="dialog-item">
                                    <div className="item-left">
                                        <i>*</i>
                                        <span>{intl.get('DATABASE.HTTPFS_ADDRESS')}：</span>
                                    </div>
                                    <div className="item-right">
                                        <input
                                            defaultValue=''
                                            placeholder={intl.get('DATABASE.HTTPFS_ADDRESS')}
                                            required="required"
                                            name="httpfs"
                                            className="tp-input dialog-input"
                                            onChange={this.handleChange}
                                        />
                                        <Tooltip
                                            placement="topRight"
                                            title={intl.get('DATABASE.'+ connectDefaultInfo[connectionTypes.hdfs].httpfs.tip)}
                                        >
                                            <i className="icon icon-infor after-icon"/>
                                        </Tooltip>
                                    </div>
                                </div>
                                {/*<div className="dialog-item">
                                    <div className="item-left">
                                        <span>{intl.get('DATABASE.DEFAULT')}inceptor{intl.get('DATABASE.CONNECTION')}：</span>
                                    </div>
                                    <div className="item-right">
                                        <Select
                                            options={this.state.connectionNames}
                                            width={420}
                                            handleSelect={(argus)=>this.setSelectConnection(argus)}
                                        />
                                        <Tooltip
                                            placement="topRight"
                                            title={intl.get('DATABASE.'+ connectDefaultInfo[connectionTypes.hdfs].defaultIncConnect.tip)}
                                        >
                                            <i className="icon icon-infor after-icon"/>
                                        </Tooltip>
                                    </div>
                                </div>*/}
                            </div>
                            <div className="dialog-item">
                                <div className="item-left">
                                    <span>&nbsp;</span>
                                </div>
                                <div className="item-right">
                                    <button
                                        className="test-connect"
                                        className="tp-btn tp-btn-middle tp-btn-primary"
                                        onClick={ag=> self.testConnection(ag)}>
                                        {/*<i className="icon icon-connect-test"/>*/}
                                        <span>{intl.get('DATABASE.TEST_CONN')}</span>
                                    </button>
                                    <div
                                        id="test-add-connect-tip"
                                        style={{position: 'absolute', right: 0, top: 2}}>
                                    </div>
                                </div>
                            </div>
                        </div>
                        <div className="error" id="add-connect-error-tip"></div>
                        <div className="popup-footer">
                            <button
                                disabled={this.state.disabled}
                                className="tp-btn tp-btn-middle tp-btn-primary"
                                onClick={this.submit}
                            >
                                {intl.get('DATABASE.CONFIRM')}
                            </button>
                        </div>
                    </div>
                </div>
            </div>
        );
    }
}

const propTypes = {};
const defaultProps = {};

ConnectionAdd.propTypes = propTypes;
ConnectionAdd.defaultProps = defaultProps;

export default ConnectionAdd;
