import React from 'react';
import { render, unmountComponentAtNode } from 'react-dom';
import { Tooltip, Alert, Select } from 'antd';
import PropTypes from 'prop-types';
import { fetchInceptorConnectList, fetchCreateHDFSConnect } from '../actions';

class CreateHDFSConnect extends React.Component {
    constructor (props, context) {
        super(props);
        this.state = {
            connections: [],
            connectionId:''
        };

        this.submit = this.submit.bind(this);
        this.closeDialog = this.closeDialog.bind(this);
        this.showDialog = this.showDialog.bind(this);
        this.onChange = this.onChange.bind(this);
    }

    componentDidMount () {
        this.fetchConnectionNames();
    }

    closeDialog () {
        unmountComponentAtNode(document.getElementById('popup_root'));
    }

    showDialog() {
        this.refs.popupCreateHDFSConnect.style.display = 'flex';
    }

    onChange (value) {
        this.setState({
            connectionId: value
        });
    }

    submit () {
        const { dispatch } = this.props;
        const connectObj = {
            connection_name: this.refs.connectionName.value,
            description: this.refs.connectionDes.value,
            httpfs: this.refs.connectionHttp.value,
            database_id: this.state.connectionId
        };
        const callback = (success) => {
            if(success) {
                unmountComponentAtNode(document.getElementById('popup_root'));
            }else {
            }
        };
        dispatch(fetchCreateHDFSConnect(connectObj, callback));
    }

    fetchConnectionNames () {
        const { dispatch } = this.props;
        const me = this;
        const callback = (success, data) => {
            if(success) {
                me.setState({
                    connections: data
                });
            }
        };
        dispatch(fetchInceptorConnectList(callback));
    }

    render () {
        const options = this.state.connections.map(connection => {
            return <Option key={connection.database_name}>{connection.database_name}</Option>
        });
        return (
            <div className="popup" ref="popupCreateHDFSConnect">
                <div className="popup-dialog popup-md">
                    <div className="popup-content">
                        <div className="popup-header">
                            <div className="header-left">
                                <i className='icon icon-connect'/>
                                <span>创建HDFS链接</span>
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
                                    <input className="tp-input dialog-input"
                                           defaultValue=""
                                           ref="connectionName"/>
                                </div>
                            </div>
                            <div className="dialog-item">
                                <div className="item-left">
                                    <span>描述：</span>
                                </div>
                                <div className="item-right">
                                    <textarea className="tp-textarea dialog-area"
                                              defaultValue=""
                                              ref="connectionDes"/>
                                </div>
                            </div>
                            <div className="dialog-item">
                                <div className="item-left">
                                    <span>httpfs地址：</span>
                                </div>
                                <div className="item-right">
                                    <input className="tp-input dialog-input"
                                           defaultValue=""
                                           ref="connectionHttp"/>
                                </div>
                            </div>
                            <div className="dialog-item">
                                <div className="item-left">
                                    <span>inceptor连接：</span>
                                </div>
                                <div className="item-right">
                                    <Select
                                        style={{ width: '100%' }}
                                        onChange={this.onChange}
                                    >
                                        {options}
                                    </Select>
                                </div>
                            </div>
                        </div>
                        <div className="popup-footer">
                            <button
                                className="tp-btn tp-btn-middle tp-btn-primary"
                                onClick={this.submit}>
                                确定
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

CreateHDFSConnect.propTypes = propTypes;
CreateHDFSConnect.defaultProps = defaultProps;

export default CreateHDFSConnect;
