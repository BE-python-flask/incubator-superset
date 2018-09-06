/**
 * Created by haitao on 17-5-11.
 */
import React from 'react';
import ReactDOM from 'react-dom';
import { fetchAddDashboard, setDashAddConfirmState } from '../actions';
import { Select, Alert, Tooltip } from 'antd';
import PropTypes from 'prop-types';
import intl from 'react-intl-universal';
import { renderAlertErrorInfo } from '../../utils/utils';

class DashboardAdd extends React.Component {
    constructor(props) {
        super(props);
        this.state = {
            selectedSlices: [],
            enableConfirm: false,
            exception: {},
            selectedSliceObjs: []
        };
        // bindings
        this.confirm = this.confirm.bind(this);
        this.closeAlert = this.closeAlert.bind(this);

        this.handleTitleChange = this.handleTitleChange.bind(this);
        this.handleDescriptionChange = this.handleDescriptionChange.bind(this);
        this.onSelectChange = this.onSelectChange.bind(this);
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

    handleDescriptionChange(e) {
        this.props.dashboard.description = e.target.value;
        this.setState({
            dashboard: this.props.dashboard
        });
        this.closeAlert('add-dashboard-error-tip');
    }

    onSelectChange(value) {
        const arr = [];
        const availableSlices = this.props.availableSlices;
        value.forEach(v => {
            availableSlices.forEach(i => {
                if (i.slice_name === v) {
                    arr.push(i);
                }
            });
        })
        this.setState({
            selectedSlices: value,
            selectedSliceObjs: arr
        });
        this.closeAlert('add-dashboard-error-tip');
    }

    confirm() {
        const self = this;
        const { dispatch, availableSlices } = self.props;
        dispatch(fetchAddDashboard(self.state, availableSlices, callback));
        function callback(success, message) {
            if(success) {
                self.closeAlert("popup_root");
            }else {
                renderAlertErrorInfo(message, 'add-dashboard-error-tip', '100%', self);
            }
        }
    }

    render() {
        const self = this;
        const Option = Select.Option;
        const options = self.props.availableSlices.map(d => {
            return <Option key={d.slice_name}>{d.slice_name}</Option>
        });

        return (
            <div className="popup">
                <div className="popup-dialog popup-md">
                    <div className="popup-content">
                        <div className="popup-header">
                            <div className="header-left">
                                <i className="icon icon-dashboard-popup" />
                                <span>{intl.get('DASHBOARD.ADD_DASHBOARD')}</span>
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
                                    <i>*</i>
                                    <span>{intl.get('DASHBOARD.TITLE')}：</span>
                                </div>
                                <div className="item-right">
                                    <input
                                        className="tp-input dialog-input"
                                        value={this.props.dashboard.name}
                                        onChange={this.handleTitleChange}
                                        autoFocus
                                    />
                                </div>
                            </div>
                            <div className="dialog-item">
                                <div className="item-left">
                                    <span>{intl.get('DASHBOARD.DESCRIPTION')}：</span>
                                </div>
                                <div className="item-right">
                                    <textarea
                                        className="tp-textarea dialog-area"
                                        value={this.props.dashboard.description}
                                        onChange={this.handleDescriptionChange}
                                    />
                                </div>
                            </div>
                            <div className="dialog-item">
                                <div className="item-left">
                                    <span>{intl.get('DASHBOARD.SLICE')}：</span>
                                </div>
                                <div className="item-right">
                                    <div id="edit_pop_select">
                                        <Select mode={'multiple'}
                                            value={self.state.selectedSlices}
                                            style={{ width: '100%' }}
                                            placeholder={intl.get('DASHBOARD.SELECT_SLICE')}
                                            onChange={this.onSelectChange}>
                                            {options}
                                        </Select>
                                    </div>
                                    <Tooltip
                                        title={intl.get('DASHBOARD.SELECT_SLICE_TIP')}
                                        placement="topRight"
                                    >
                                        <i className="icon icon-info after-icon" />
                                    </Tooltip>
                                </div>
                            </div>
                        </div>
                        <div className="error" id="add-dashboard-error-tip"></div>
                        <div className="popup-footer">
                            <button
                                className="tp-btn tp-btn-middle tp-btn-primary"
                                onClick={this.confirm}
                                disabled={!this.state.enableConfirm}
                            >
                                {intl.get('DASHBOARD.CONFIRM')}
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

DashboardAdd.propTypes = propTypes;
DashboardAdd.defaultProps = defaultProps;

export default DashboardAdd;