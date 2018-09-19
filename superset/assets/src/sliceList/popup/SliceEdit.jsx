/**
 * Created by haitao on 17-5-11.
 */
import React from 'react';
import ReactDOM from 'react-dom';
import { fetchUpdateSlice, fetchDashboardList } from '../actions';
import { Select, Alert, Tooltip } from 'antd';
import PropTypes from 'prop-types';
import intl from 'react-intl-universal';
import { renderAlertErrorInfo, renderGlobalErrorMsg } from '../../utils/utils';

class SliceEdit extends React.Component {
    constructor(props) {
        super(props);
        this.state = {
            exception: {},
            enableConfirm: true,
            sliceDetail: {},
            availableDashboards: [],
            dashboardOptions: [],
            selectedDashboards: initDefaultOptions(),
            selectedDashboardObjs: props.sliceDetail.dashboards
        };

        function initDefaultOptions() {
            let defaultOptions = [];
            props.sliceDetail.dashboards.map(dashboard => {
                defaultOptions.push(dashboard.name);
            });
            return defaultOptions;
        }
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
        this.props.sliceDetail.slice_name = e.currentTarget.value;
        let enableConfirm = false;
        if(e.currentTarget.value && e.currentTarget.value.length > 0) {
            enableConfirm = true;
        }
        this.setState({
            sliceDetail: this.props.sliceDetail,
            enableConfirm: enableConfirm
        });
        this.closeAlert('edit-slice-error-tip');
    }

    handleDescriptionChange(e) {
        this.props.sliceDetail.description = e.currentTarget.value;
        this.setState({
            sliceDetail: this.props.sliceDetail
        });
        this.closeAlert('edit-slice-error-tip');
    }

    onSelectChange(value) {
        const arr = [];
        const availableDashboards = this.state.availableDashboards;
        value.forEach(v => {
            availableDashboards.forEach(i => {
                if (i.name ===v) {
                    arr.push(i);
                }
            })
        });
        this.setState({
            selectedDashboards: value,
            selectedDashboardObjs: arr
        });
        this.closeAlert('edit-slice-error-tip');
    }

    confirm() {
        const self = this;
        const { dispatch } = self.props;
        dispatch(fetchUpdateSlice(self.state, self.props.sliceDetail, callback));
        function callback(success, message) {
            if(success) {
                self.closeAlert('popup_root');
            }else {
                renderAlertErrorInfo(message, 'edit-slice-error-tip', '100%', self);
            }
        }
    }

    componentDidMount() {
        const self = this;
        fetchDashboardList(callback);
        function callback(success, data) {
            if(success) {
                const availableOptions = data.data.map(dashboard => {
                    return <Option key={dashboard.name}>
                        {dashboard.name}</Option>
                });
                self.setState({
                    availableDashboards: data.data,
                    dashboardOptions: availableOptions
                });
            }else {
                renderGlobalErrorMsg(data);
            }
        }
    }

    render() {
        const sliceDetail = this.props.sliceDetail;
        const {dashboardOptions, selectedDashboards, enableConfirm} = this.state;

        return (
            <div className="popup">
                <div className="popup-dialog popup-md">
                    <div className="popup-content">
                        <div className="popup-header">
                            <div className="header-left">
                                <i className="icon icon-slice-popup" />
                                <span>{intl.get('SLICE.EDIT_SLICE')}</span>
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
                                    <span>{intl.get('SLICE.NAME')}：</span>
                                </div>
                                <div className="item-right">
                                    <input
                                        className="tp-input dialog-input"
                                        value={sliceDetail.slice_name}
                                        onChange={this.handleTitleChange}
                                        autoFocus
                                    />
                                </div>
                            </div>
                            <div className="dialog-item">
                                <div className="item-left">
                                    <span>{intl.get('SLICE.DESCRIPTION')}：</span>
                                </div>
                                <div className="item-right">
                                    <textarea
                                        className="tp-textarea dialog-area"
                                        value={sliceDetail.description || ''}
                                        onChange={this.handleDescriptionChange}
                                    />
                                </div>
                            </div>
                            <div className="dialog-item">
                                <div className="item-left">
                                    <span>{intl.get('SLICE.DASHBOARD')}：</span>
                                </div>
                                <div className="item-right">
                                    <div id="edit_pop_select">
                                        <Select mode={'multiple'}
                                            style={{ width: '100%' }}
                                            defaultValue={selectedDashboards}
                                            placeholder={intl.get('SLICE.SELECT_DASHBOARD')}
                                            onChange={this.onSelectChange}
                                        >
                                            {dashboardOptions}
                                        </Select>
                                    </div>
                                    <Tooltip
                                        title={intl.get('SLICE.SELECT_DASHBOARD_TIP')}
                                        placement="topRight"
                                    >
                                        <i className="icon icon-info after-icon"/>
                                    </Tooltip>
                                </div>
                            </div>
                            <div className="dialog-item">
                                <div className="sub-item">
                                    <div className="item-left">
                                        <span>{intl.get('SLICE.CREATOR')}：</span>
                                    </div>
                                    <div className="item-right">
                                        <span>{sliceDetail.created_by_user}</span>
                                    </div>
                                </div>
                                <div className="sub-item">
                                    <div className="item-left">
                                        <span>{intl.get('SLICE.MODIFIER')}：</span>
                                    </div>
                                    <div className="item-right">
                                        <span>{sliceDetail.changed_by_user}</span>
                                    </div>
                                </div>
                            </div>
                            <div className="dialog-item">
                                <div className="sub-item">
                                    <div className="item-left">
                                        <span>{intl.get('SLICE.CREATE_DATE')}：</span>
                                    </div>
                                    <div className="item-right">
                                        <span>{sliceDetail.created_on}</span>
                                    </div>
                                </div>
                                <div className="sub-item">
                                    <div className="item-left">
                                        <span>{intl.get('SLICE.MODIFY_TIME')}：</span>
                                    </div>
                                    <div className="item-right">
                                        <span>{sliceDetail.changed_on}</span>
                                    </div>
                                </div>
                            </div>
                        </div>
                        <div className="error" id="edit-slice-error-tip"></div>
                        <div className="popup-footer">
                            <button
                                className="tp-btn tp-btn-middle tp-btn-primary"
                                onClick={this.confirm}
                                disabled={!enableConfirm}
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

SliceEdit.propTypes = propTypes;
SliceEdit.defaultProps = defaultProps;

export default SliceEdit;