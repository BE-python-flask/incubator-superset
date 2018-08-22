import React from 'react';
import ReactDOM from 'react-dom';
import {render} from 'react-dom';
import {connectionTypes} from '../actions';
import * as actions from '../actions';
import * as utils from '../../utils/utils';
import intl from 'react-intl-universal';
import {Alert, Tooltip, message} from 'antd';
import {Select} from '../components';
import PropTypes from 'prop-types';
import {isCorrectConnection, argsValidate, connectDefaultInfo} from '../utils';

class ConnectionEdit extends React.Component {
    constructor(props) {
        super(props);
        this.state = {
            exception: {},
            disabled: false,
            database: this.props.database,
            connected: false,
            connectionNames: [],
            id: props.connectionId
        };

        // bindings
        this.confirm = this.confirm.bind(this);
        this.testConnection = this.testConnection.bind(this);
        this.closeAlert = this.closeAlert.bind(this);
        // this.onSelectChange = this.onSelectChange.bind(this);
        this.argsOnBlur = this.argsOnBlur.bind(this);
        this.handleInputChange = this.handleInputChange.bind(this);
    };

    componentDidMount () {
        let database = {};
        if(isCorrectConnection(this.props.connectionType, connectionTypes)) {
            let connectParams = JSON.stringify(JSON.parse(this.state.database.args), undefined, 4);
            document.getElementById('connectParams').value = connectParams;
            database = {
                ...this.state.database,
                databaseArgs: connectParams,
                connectionType: this.props.connectionType
            };

        }else if(this.props.connectionType === connectionTypes.hdfs) {
            database = {
                ...this.state.database,
                connectionType: this.props.connectionType
            };
            this.fetchConnectionNames();
        }
        this.setState({
            database: database
        });
    }

    fetchConnectionNames () {
        const self = this;
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
        this.props.dispatch(actions.fetchConnectionNames(callback));
    }

    closeAlert(id) {
        ReactDOM.unmountComponentAtNode(document.getElementById(id));
    }

    testConnection(testCallBack) {
        const self = this;
        const { dispatch, connectionType } = self.props;
        if(isCorrectConnection(connectionType, connectionTypes)) {
            if(!argsValidate(this.state.database.databaseArgs)) {
                utils.renderAlertErrorInfo(
                    intl.get('DATABASE.CONN_GRAMMAR_ERROR'),
                    'edit-connect-error-tip', '100%', this
                );
                return;
            }
            dispatch(actions.testConnection(
                {
                    database_name: self.state.database.database_name,
                    sqlalchemy_uri: self.state.database.sqlalchemy_uri,
                    args: self.state.database.databaseArgs
                }, callback)
            );
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
            } else {
                exception.type = "error";
                exception.message = intl.get('DATABASE.CONN_IS_UNILLEGAL');
                connected = false;
                utils.renderAlertErrorInfo(message, 'edit-connect-error-tip', '100%', self);
            }
            self.setState({
                exception: exception,
                connected: connected
            });
            let connectType = connectionTypes.hdfs;
            if(isCorrectConnection(connectionType, connectionTypes)) {
                connectType = connectionTypes.inceptor;
            }
            utils.renderAlertTip(exception, 'test-connect-tip-' + connectType, '100%');
            if(typeof testCallBack === 'function') {
                testCallBack(self.state.connected);
            }
        }
    }

    handleInputChange (e) {
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
        if(isCorrectConnection(this.props.connectionType, connectionTypes)) {
            if((database.database_name && database.database_name.length > 0) &&
                (database.sqlalchemy_uri && database.sqlalchemy_uri.length > 0) &&
                argsValidate(database.databaseArgs)) {
                disabled = false;
            }else {
                disabled = true;
            }
        }else if(this.props.connectionType === connectionTypes.hdfs) {
            if((database.connection_name && database.connection_name.length > 0) &&
                (database.httpfs && database.httpfs.length > 0)) {
                disabled = false;
            }else {
                disabled = true;
            }
        }

        this.setState({
            disabled: disabled
        });

        this.closeAlert('edit-connect-error-tip');
    }

    argsOnBlur() {
        const args = this.state.database.databaseArgs;
        if(!argsValidate(args)) {
            utils.renderAlertErrorInfo(
                intl.get('DATABASE.CONN_GRAMMAR_ERROR'),
                'edit-connect-error-tip',
                '100%',
                 this
            );
        }
    }

/*    onSelectChange(databaseId) {
        const database = {
            ...this.state.database,
            database_id: databaseId
        };
        this.setState({
            database: database
        });
    }*/

    doUpdateConnection() {
        const {dispatch} = this.props;
        const self = this;
        dispatch(actions.fetchUpdateConnection(self.state.database, callback));
        function callback(success, message) {
            if(success) {
                self.closeAlert("popup_root");
            }else {
                utils.renderAlertErrorInfo(message, 'edit-connect-error-tip', '100%', self);
            }
        }
    }

    confirm() {
        const self = this;
        if(this.state.connected) {
            this.doUpdateConnection();
        }else {
            this.testConnection(testCallBack);
            function testCallBack(success, data) {
                if(success) {
                    self.doUpdateConnection();
                }else {
                    utils.renderGlobalErrorMsg(data);
                }
            }
        }
    }

    render() {
        const connectionType = this.props.connectionType;
        const {connectionNames, database}= this.state;

/*        console.log(connectionType);
        console.log(connectDefaultInfo[connectionType]);
        console.log(connectDefaultInfo[connectionType].str.tip);
        console.log('DATABASE.' + connectDefaultInfo[connectionType].str.tip);

        console.log('value is:', intl.get('DATABASE.' + connectDefaultInfo[connectionType].str.tip));
        console.log(intl.get('DATABASE.' + connectDefaultInfo[connectionType].args.tip));
        console.log('----------------------------------');*/

        return (
            <div className="popup">
                <div className="popup-dialog popup-md">
                    <div className="popup-content">
                        <div className="popup-header">
                            <div className="header-left">
                                <i className="icon icon-connect" />
                                <span>{intl.get('DATABASE.EDIT') + ' '}{connectionType}{' '+ intl.get('DATABASE.CONNECTION')}</span>
                            </div>
                            <div className="header-right">
                                <i
                                    className="icon icon-close"
                                    onClick={argus => this.closeAlert('popup_root')}
                                />
                            </div>
                        </div>
                        <div className="popup-body">
                            {/*S: inceptor connection body*/}
                            <div className={isCorrectConnection(connectionType, connectionTypes)?'':'none'} >
                                <div className="dialog-item">
                                    <div className="item-left">
                                        <span>{intl.get('DATABASE.CONN_TYPE')}：</span>
                                    </div>
                                    <div className="item-right">
                                        <span>{connectionType}</span>
                                    </div>
                                </div>
                                <div className="dialog-item">
                                    <div className="item-left">
                                        <i>*</i>
                                        <span>{intl.get('DATABASE.CONN_TYPE')}：</span>
                                    </div>
                                    <div className="item-right">
                                        <input
                                            name="database_name"
                                            className="tp-input dialog-input"
                                            value={database.database_name}
                                            onChange={this.handleInputChange}
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
                                        name = "description"
                                        className="tp-textarea dialog-area"
                                        value={database.description||''}
                                        onChange={this.handleInputChange}
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
                                            name = "sqlalchemy_uri"
                                            className="tp-input dialog-input"
                                            value={database.sqlalchemy_uri}
                                            onChange={this.handleInputChange}
                                        />
                                        <Tooltip
                                            placement="topRight"
                                            title={intl.get('DATABASE.' + connectDefaultInfo[connectionType].str.tip)}
                                        >
                                            <i className="icon icon-info after-icon" />
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
                                            id="connectParams"
                                            name="databaseArgs"
                                            style={{height:'120px'}}
                                            className="tp-textarea dialog-area"
                                            onChange={this.handleInputChange}
                                            onBlur={this.argsOnBlur}
                                            disabled={connectionType===connectionTypes.inceptor?false:true}
                                        />
                                        <Tooltip
                                            placement="topRight"
                                            title={intl.get('DATABASE.' + connectDefaultInfo[connectionType].args.tip)}
                                         >
                                            <i
                                                className="icon icon-info after-textarea-icon"
                                                style={{top: 50}}
                                            />
                                        </Tooltip>
                                    </div>
                                </div>
                                <div className="dialog-item" style={{ position: 'relative' }}>
                                    <div className="item-left"></div>
                                    <div className="item-right item-connect-test">
                                        <button 
                                            className="tp-btn tp-btn-middle tp-btn-primary"
                                            onClick={this.testConnection}>
                                            <span>{intl.get('DATABASE.TEST_CONN')}</span>
                                        </button>
                                        <div id='test-connect-tip-INCEPTOR'></div>
                                    </div>
                                </div>
                                <div className="dialog-item">
                                    <div className="sub-item">
                                        <div className="item-left">
                                            <span>{intl.get('DATABASE.CREATOR')}：</span>
                                        </div>
                                        <div className="item-right">
                                            <span>{database.created_by_user}</span>
                                        </div>
                                    </div>
                                    <div className="sub-item">
                                        <div className="item-left">
                                            <span>{intl.get('DATABASE.MODIFIER')}：</span>
                                        </div>
                                        <div className="item-right">
                                            <span>{database.changed_by_user}</span>
                                        </div>
                                    </div>
                                </div>
                                <div className="dialog-item">
                                    <div className="sub-item">
                                        <div className="item-left">
                                            <span>{intl.get('DATABASE.CREATE_DATE')}：</span>
                                        </div>
                                        <div className="item-right">
                                            <span>{database.created_on}</span>
                                        </div>
                                    </div>
                                    <div className="sub-item">
                                        <div className="item-left">
                                            <span>{intl.get('DATABASE.MODIFY_TIME')}：</span>
                                        </div>
                                        <div className="item-right">
                                            <span>{database.changed_on}</span>
                                        </div>
                                    </div>
                                </div>
                            </div>
                            {/*E: inceptor connection body*/}

                            {/*S: HDFS connection body*/}
                            <div className={connectionType===connectionTypes.hdfs?'':'none'} >
                                <div className="dialog-item">
                                    <div className="item-left">
                                        <span>{intl.get('DATABASE.CONN_TYPE')}：</span>
                                    </div>
                                    <div className="item-right">
                                        <span>{connectionType}</span>
                                    </div>
                                </div>
                                <div className="dialog-item">
                                    <div className="item-left">
                                        <i>*</i>
                                        <span>{intl.get('DATABASE.CONN_NAME')}：</span>
                                    </div>
                                    <div className="item-right">
                                        <input
                                            className="tp-input dialog-input"
                                            name="connection_name"
                                            value={database.connection_name}
                                            onChange={this.handleInputChange}
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
                                        className="tp-textarea dialog-area"
                                        name="description"
                                        value={database.description||''}
                                        onChange={this.handleInputChange}
                                    />
                                    </div>
                                </div>
                                <div className="dialog-item">
                                    <div className="item-left">
                                        <i>*</i>
                                        <span>httpfs：</span>
                                    </div>
                                    <div className="item-right">
                                        <input
                                            className="tp-input dialog-input"
                                            name="httpfs"
                                            value={database.httpfs}
                                            onChange={this.handleInputChange}
                                        />
                                        <Tooltip
                                            placement="topRight"
                                            title={intl.get('DATABASE.'+ connectDefaultInfo[connectionType].httpfs.tip)}
                                        >
                                            <i className="icon icon-info after-icon" />
                                        </Tooltip>
                                    </div>
                                </div>
                                <div className="dialog-item" style={{ position: 'relative' }}>
                                    <div className="item-left"></div>
                                    <div className="item-right item-connect-test">
                                        <button 
                                            className="tp-btn tp-btn-middle tp-btn-primary"
                                            onClick={this.testConnection}>
                                            <span>{intl.get('DATABASE.TEST_CONN')}</span>
                                        </button>
                                        <div id='test-connect-tip-HDFS'></div>
                                    </div>
                                </div>
                                {/*<div className="dialog-item">
                                    <div className="item-left">
                                        <span style={{whiteSpace:'nowrap'}}>{intl.get('DATABASE.INCEPTOR_CONNECTION')}：</span>
                                    </div>
                                    <div className="item-right">
                                        <Select
                                            options={connectionNames}
                                            value={database.database}
                                            width={420}
                                            handleSelect={(argus)=>this.onSelectChange(argus)}
                                        />
                                        <Tooltip
                                            placement="topRight"
                                            title={intl.get('DATABASE.' + connectDefaultInfo[connectionType].defaultIncConnect.tip)}
                                        >
                                            <i className="icon icon-info after-icon" />
                                        </Tooltip>
                                    </div>
                                </div>*/}
                                <div className="dialog-item">
                                    <div className="sub-item">
                                        <div className="item-left">
                                            <span>{intl.get('DATABASE.CREATOR')}：</span>
                                        </div>
                                        <div className="item-right">
                                            <span>{database.created_by_user}</span>
                                        </div>
                                    </div>
                                    <div className="sub-item">
                                        <div className="item-left">
                                            <span>{intl.get('DATABASE.MODIFIER')}：</span>
                                        </div>
                                        <div className="item-right">
                                            <span>{database.changed_by_user}</span>
                                        </div>
                                    </div>
                                </div>
                            </div>
                            {/*E: HDFS connection*/}
                        </div>
                        <div className="error" id="edit-connect-error-tip"></div>
                        <div className="popup-footer">
                            <button
                                className="tp-btn tp-btn-middle tp-btn-primary"
                                onClick={this.confirm}
                                disabled={this.state.disabled}
                            >{intl.get('DATABASE.CONFIRM')}</button>
                        </div>
                    </div>
                </div>
            </div>
        );
    }
}

const propTypes = {};
const defaultProps = {};

ConnectionEdit.propTypes = propTypes;
ConnectionEdit.defaultProps = defaultProps;

export default ConnectionEdit;
