import React from 'react';
import ReactDOM from 'react-dom';
import {Alert} from 'antd';
import PropTypes from 'prop-types';

import {WarningAlert} from './WarningAlert';
import {ErrorAlert} from './ErrorAlert';
import {loadIntlResources} from '../../utils/utils';

import intl from 'react-intl-universal';

class ConfirmModal extends React.Component {
    constructor(props) {
        super(props);
        this.state = {
            initDone: false
        };
        // bindings
        this.confirm = this.confirm.bind(this);
        this.closeDialog = this.closeDialog.bind(this);
    };

    closeDialog() {
        ReactDOM.unmountComponentAtNode(document.getElementById("popup_root"));
    }

    confirm() {
        if(this.props.needCallback) {
            this.props.confirmCallback();
        }
        this.closeDialog();
    }

    componentDidMount() {
        loadIntlResources(_ => this.setState({ initDone: true }), 'popup');
    }

    render() {
        const {confirmMessage, confirmType} = this.props;
        let alert = <WarningAlert message={confirmMessage}/>;
        if(confirmType === 'error') {
            alert = <ErrorAlert message={confirmMessage}/>
        }
        return (this.state.initDone &&
            <div className="popup" style={{zIndex: 1250}}>
                <div className="popup-dialog popup-sm">
                    <div className="popup-content">
                        <div className="popup-header">
                            <div className="header-left">
                                <i className="icon icon-dashboard-popup" />
                                <span>{intl.get('POPUP.CONFIRM')}</span>
                            </div>
                            <div className="header-right">
                                <i
                                    className="icon icon-close"
                                    onClick={this.closeDialog}
                                />
                            </div>
                        </div>
                        <div className="popup-body">
                            {alert}
                        </div>
                        <div className="popup-footer">
                            <button
                                className="tp-btn tp-btn-middle tp-btn-primary"
                                onClick={this.confirm}
                            >
                                {intl.get('POPUP.CONFIRM')}
                            </button>
                        </div>
                    </div>
                </div>
            </div>
        );
    }
}

export default ConfirmModal;