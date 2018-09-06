/**
 * Created by haitao on 17-5-11.
 */
import React from 'react';
import ReactDOM from 'react-dom';
import { fetchAvailableSlices, fetchUpdateDashboard } from '../actions';
import { Select, Alert, Tooltip } from 'antd';
import PropTypes from 'prop-types';
import { renderAlertErrorInfo, renderGlobalErrorMsg, fetchDatabaseList } from '../../utils/utils';
import intl from 'react-intl-universal';

class DashboardEdit extends React.Component {
    constructor(props) {
        super(props);
        function initDefaultOptions() {
            let selectedSlices = [];
            props.dashboardDetail.slices.map(slice => {
                selectedSlices.push(slice.slice_name);
            });
            return selectedSlices;
        };

        this.state = {
            exception: {},
            enableConfirm: true,
            dashboardDetail: {
                description: ''
            },
            availableSlices: [],
            sliceOptions: [],
            selectedSlices: initDefaultOptions(),

            selectedSliceObjs: props.dashboardDetail.slices       //for get the ids
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
        this.props.dashboardDetail.name = e.currentTarget.value;
        let enableConfirm = false;
        if(e.currentTarget.value && e.currentTarget.value.length > 0) {
            enableConfirm = true;
        }
        this.setState({
            dashboardDetail: this.props.dashboardDetail,
            enableConfirm: enableConfirm
        });
        this.closeAlert('edit-dashboard-error-tip');
    }

    handleDescriptionChange(e) {
        this.props.dashboardDetail.description = e.currentTarget.value;
        this.setState({
            dashboardDetail: this.props.dashboardDetail
        });
        this.closeAlert('edit-dashboard-error-tip');
    }

    onSelectChange(value) {
        const arr = [];
        const availableSlices = this.state.availableSlices;
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
        this.closeAlert('edit-dashboard-error-tip');
    }

    confirm() {
        const self = this;
        const { dispatch } = self.props;
        dispatch(fetchUpdateDashboard(self.state, self.props.dashboardDetail, callback));
        function callback(success, message) {
            if(success) {
                self.closeAlert('popup_root');
            }else {
                renderAlertErrorInfo(message, 'edit-dashboard-error-tip', '100%', self);
            }
        }
    }

    componentDidMount() {
        const callback = (success, response) => {
            if(success) {
                const Option = Select.Option;
                const options = response.data.map(slice => {
                    return <Option key={slice.slice_name}>{slice.slice_name}</Option>
                });
                this.setState({
                    availableSlices: response.data,
                    sliceOptions: options
                });
            }else {
                renderGlobalErrorMsg(response);
            }
        };
        fetchAvailableSlices(callback);
    }

    render() {

        const { selectedSlices, sliceOptions } = this.state;

        return (
            <div className="popup">
                <div className="popup-dialog popup-md">
                    <div className="popup-content">
                        <div className="popup-header">
                            <div className="header-left">
                                <i className="icon icon-dashboard-popup" />
                                <span>{intl.get('DASHBOARD.EDIT_DASHBOARD')}</span>
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
                                        value={this.props.dashboardDetail.name}
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
                                        value={this.props.dashboardDetail.description}
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
                                            style={{ width: '100%' }}
                                            defaultValue={selectedSlices}
                                            placeholder={intl.get('DASHBOARD.SELECT_SLICE')}
                                            onChange={this.onSelectChange}
                                        >
                                            {sliceOptions}
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
                            {/*<div className="dialog-item">
                                <div className="item-left">
                                    <span>{intl.get('DASHBOARD.DATASET')}：</span>
                                </div>
                                <div className="item-right">
                                    <input
                                        className="tp-input dialog-input"
                                        value={this.props.dashboardDetail.table_names}
                                        disabled
                                    />
                                </div>
                            </div>*/}
                        </div>
                        <div className="error" id="edit-dashboard-error-tip"></div>
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

DashboardEdit.propTypes = propTypes;
DashboardEdit.defaultProps = defaultProps;

export default DashboardEdit;