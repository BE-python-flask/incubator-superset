import React from 'react';
import ReactDOM from 'react-dom';
import PropTypes from 'prop-types';
import { Alert } from 'antd';
import { selectRows, applyDelete } from '../actions';
import intl from 'react-intl-universal';
import { renderAlertErrorInfo } from '../../utils/utils';

import { WarningAlert } from '../../common/components/WarningAlert';

class ConnectionDelete extends React.Component {
    constructor(props) {
        super(props);
        this.state = {
            exception: {}
        };
        // bindings
        this.confirm = this.confirm.bind(this);
        this.closeAlert = this.closeAlert.bind(this);
    }

    closeAlert(mountId) {
        ReactDOM.unmountComponentAtNode(document.getElementById(mountId));
    }

    confirm() {
        const self = this;
        const {dispatch, deleteType} = this.props;
        const callback = (success, message) => {
            if(success) {
                self.closeAlert('popup_root');
            }else {
                renderAlertErrorInfo(
                    message,
                    'delete-database-error-tip',
                    '100%',
                    self
                );
            }
        };

        if(deleteType === "none") {
            this.closeAlert('popup_root');
        } else {
            dispatch(applyDelete(callback));
        }
    }

    render() {
        return (
            <div className="popup">
                <div className="popup-dialog popup-md">
                    <div className="popup-content">
                        <div className="popup-header">
                            <div className="header-left">
                                <i className="icon icon-trash" />
                                <span>{intl.get('DATABASE.DELETE_CONN')}</span>
                            </div>
                            <div className="header-right">
                                <i
                                    className="icon icon-close"
                                    onClick={argus => this.closeAlert('popup_root')}
                                />
                            </div>
                        </div>
                        <div className="popup-body">
                            <WarningAlert
                                message={this.props.deleteTips}
                            />
                        </div>
                        <div className="error" id="delete-database-error-tip"></div>
                        <div className="popup-footer">
                            <button
                                className="tp-btn tp-btn-middle tp-btn-primary"
                                onClick={this.confirm}>
                                {intl.get('DATABASE.CONFIRM')}
                            </button>
                        </div>
                    </div>
                </div>
            </div>
        );
    }
}

ConnectionDelete.propTypes = {};
ConnectionDelete.defaultProps = {};

export default ConnectionDelete;
