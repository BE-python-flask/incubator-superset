import React from 'react';
import ReactDOM from 'react-dom';
import PropTypes from 'prop-types';
import { Alert } from 'antd';
import { fetchLists, fetchSliceDelete, fetchSliceDeleteMul } from '../actions';
import { renderAlertErrorInfo } from '../../utils/utils';
import intl from 'react-intl-universal';
import { WarningAlert } from '../../common/components/WarningAlert';

class SliceDelete extends React.Component {
    constructor(props) {
        super(props);
        this.state = {};
        // bindings
        this.confirm = this.confirm.bind(this);
        this.closeAlert = this.closeAlert.bind(this);
    };


    closeAlert(id) {
        ReactDOM.unmountComponentAtNode(document.getElementById(id));
    }

    confirm() {
        const self = this;
        const { dispatch, slice, deleteType } = self.props;
        if(deleteType === "single") {
            dispatch(fetchSliceDelete(slice.id, callback));
        }else if(deleteType === "multiple") {
            dispatch(fetchSliceDeleteMul(callback));
        }else if(deleteType === "none") {
            this.closeAlert('popup_root');
        }

        function callback(success, message) {
            if(success) {
                self.closeAlert('popup_root');
            }else {
                renderAlertErrorInfo(
                    message,
                    'delete-slice-error-tip',
                    '100%',
                    self
                );
            }
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
                                <span>{intl.get('SLICE.DELETE_SLICE')}</span>
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
                        <div className="error" id="delete-slice-error-tip"></div>
                        <div className="popup-footer">
                            <button
                                className="tp-btn tp-btn-middle tp-btn-primary"
                                onClick={this.confirm}
                            >
                                {intl.get('SLICE.CONFIRM')}
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

SliceDelete.propTypes = propTypes;
SliceDelete.defaultProps = defaultProps;

export default SliceDelete;
