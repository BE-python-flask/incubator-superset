/**
 * Created by haitao on 17-5-11.
 */
import React from 'react';
import ReactDOM from 'react-dom';
import { Select, Alert } from 'antd';
import PropTypes from 'prop-types';
import intl from 'react-intl-universal';

class DetailType extends React.Component {
    constructor(props) {
        super(props);
        // bindings
        this.confirm = this.confirm.bind(this);
        this.closeDialog = this.closeDialog.bind(this);
    };

    closeDialog() {
        ReactDOM.unmountComponentAtNode(document.getElementById("popup_root"));
    }

    confirm() {
        const { dataType, setDataAccuracy } = this.props;
        const accuracy = {
            'maxLen': this.refs.maxLen.value,
            'effectiveNum': this.refs.effectiveNum.value,
            'decimalNum': this.refs.decimalNum.value
        };
        setDataAccuracy(dataType, accuracy);
        ReactDOM.unmountComponentAtNode(document.getElementById("popup_root"));
    }

    render() {
        const {dataType} = this.props;
        const title = intl.get('DATASET.SET') + dataType + intl.get('DATASET.PRECISION');
        return (
            <div className="popup" ref="popupDetailType">
                <div className="popup-dialog popup-sm">
                    <div className="popup-content">
                        <div className="popup-header">
                            <div className="header-left">
                                <i className="icon icon-dashboard-popup" />
                                <span>{title}</span>
                            </div>
                            <div className="header-right">
                                <i className="icon icon-close" onClick={this.closeDialog} />
                            </div>
                        </div>
                        <div className="popup-body" style={{padding: 20}}>
                            <div className={dataType.indexOf('varchar')>-1?'':'none'}>
                                <div className="dialog-item">
                                    <div className="item-left">
                                        <span style={{display: 'inline-block'}}>{intl.get('DATASET.MAX_STRING_LENGTH')}：</span>
                                    </div>
                                    <div className="item-right" style={{width: 300}}>
                                        <input className="form-control dialog-input" defaultValue="20" ref="maxLen"/>
                                    </div>
                                </div>
                            </div>
                            <div className={dataType.indexOf('decimal')>-1?'':'none'}>
                                <div className="dialog-item">
                                    <div className="item-left">
                                        <span>{intl.get('DATASET.VALID_NUM')}：</span>
                                    </div>
                                    <div className="item-right">
                                        <input className="form-control dialog-input" defaultValue="10" ref="effectiveNum"/>
                                    </div>
                                </div>
                                <div className="dialog-item">
                                    <div className="item-left">
                                        <span>{intl.get('DATASET.DECIMAL_NUM')}：</span>
                                    </div>
                                    <div className="item-right">
                                        <input className="form-control dialog-input" defaultValue="2" ref="decimalNum"/>
                                    </div>
                                </div>
                            </div>
                        </div>
                        <div className="popup-footer">
                            <button className="tp-btn tp-btn-middle tp-btn-primary" onClick={this.confirm}>
                                {intl.get('DATASET.CONFIRM')}
                            </button>
                        </div>
                    </div>
                </div>
            </div>
        );
    }
}

export default DetailType;