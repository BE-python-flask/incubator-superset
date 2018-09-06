import $ from 'jquery';
import React, { PropTypes } from 'react';
import { Select } from 'antd';
import intl from 'react-intl-universal';

import ModalTrigger from '../../components/ModalTrigger';
require('react-bootstrap-table/css/react-bootstrap-table.css');
import { renderGlobalErrorMsg } from '../../utils/utils';
import { fetchAvailableSlices } from '../../dashboardList/actions';
import { getSelectedSlices } from '../../utils/common';

const propTypes = {
    dashboard: PropTypes.object.isRequired,
    triggerNode: PropTypes.node.isRequired,
};

class DashboardEdit extends React.Component {
    constructor(props) {
        super(props);
        this.state = {
            dashboard: {
                description: ''
            },
            enableConfirm: true,
            selectedSlices: [],
            availableSlices: [],
            selectedSliceObjs: [],
            availableOptions: [],
            slicesLoaded: false
        };

        this.editDashboard = this.editDashboard.bind(this);
        this.handleTitleChange = this.handleTitleChange.bind(this);
        this.onChange = this.onChange.bind(this);
        this.handleDescriptionChange = this.handleDescriptionChange.bind(this);
    }

    componentDidMount() {
        this.getDefaultSlices();
        this.getAvailableSlices();
    }

    getDefaultSlices() {
        const { dashboard } = this.props;
        const url = '/dashboard/show/' + dashboard.id;
        $.ajax({
            url: url,
            type: 'GET',
            success: response => {
                const selectedSlices = [];
                response.data.slices.map(slice => {
                    selectedSlices.push(slice.slice_name);
                });
                this.setState({
                    dashboard: response.data,
                    selectedSlices: selectedSlices,
                    selectedSliceObjs: response.data.slices,
                    slicesLoaded: true,
                });
            },
            error: error => {
                this.errored = true;
                this.setState({
                    errorMsg: this.props.dashboard.getAjaxErrorMsg(error.message),
                });
            },
        });
    }

    getAvailableSlices() {
        const url = "/slice/listdata/?page_size=1000";
        $.ajax({
            url: url,
            type: 'GET',
            success: response => {
                const Option = Select.Option;
                const options = response.data.data.map(slice => {
                    return <Option key={slice.slice_name}>{slice.slice_name}</Option>
                });
                this.setState({
                    availableSlices: response.data.data,
                    availableOptions: options
                });
            },
            error: error => {
                this.errored = true;
                this.setState({
                    errorMsg: this.props.dashboard.getAjaxErrorMsg(error.message),
                });
            },
        });
    }

    handleTitleChange(e) {
        this.state.dashboard.name = e.currentTarget.value;
        let enableConfirm = false;
        if(e.currentTarget.value && e.currentTarget.value.length > 0) {
            enableConfirm = true;
        }
        this.setState({
            dashboard: this.state.dashboard,
            enableConfirm: enableConfirm
        });
    }

    handleDescriptionChange(e) {
        this.state.dashboard.description = e.currentTarget.value;
        this.setState({
            dashboard: this.state.dashboard
        });
    }

    editDashboard() {
        this.props.dashboard.editDashboard(this.state);
    }

    onChange(value) {
        const selectedSlices = getSelectedSlices(value, this.state.availableSlices);
        this.setState({
            selectedSlices: value,
            selectedSliceObjs: selectedSlices
        });
    }

    render() {
        const hideLoad = this.state.slicesLoaded || this.errored;

        const modalTitle = intl.get('DASHBOARD.EDIT_DASHBOARD');
        const modalIcon = "icon icon-dashboard-popup";
        const modalContent = (
            <div>
                <img
                    src="/static/assets/images/loading.gif"
                    className={'loading ' + (hideLoad ? 'hidden' : '')}
                    alt={hideLoad ? '' : 'loading'}
                />
                <div className={this.errored ? '' : 'hidden'}>
                    {this.state.errorMsg}
                </div>
                <div className="dialog-item">
                    <div className="item-left">
                        <span>{intl.get('DASHBOARD.TITLE')}：</span>
                    </div>
                    <div className="item-right">
                        <input
                            className="tp-input dialog-input"
                            value={this.state.dashboard.name}
                            onChange={this.handleTitleChange}
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
                            value={this.state.dashboard.description || ''}
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
                            <Select
                                mode={'multiple'}
                                style={{ width: '100%' }}
                                defaultValue={this.state.selectedSlices}
                                placeholder={intl.get('DASHBOARD.SELECT_SLICE')}
                                onChange={this.onChange}
                            >
                            {this.state.availableOptions}
                            </Select>
                        </div>
                    </div>
                </div>
                <div className="dialog-item">
                    <div className="item-left">
                        <span>{intl.get('DASHBOARD.DATASET')}：</span>
                    </div>
                    <div className="item-right">
                        <input
                            className="tp-input dialog-input"
                            value={this.state.dashboard.table_names} disabled
                        />
                    </div>
                </div>
            </div>
        );
        const modalFooter = (
            <div>
                <button
                    type="button"
                    className="tp-btn tp-btn-middle tp-btn-primary"
                    data-dismiss="modal"
                    onClick={this.editDashboard}
                    disabled={!this.state.enableConfirm}
                >
                    {intl.get('DASHBOARD.CONFIRM')}
                </button>
            </div>
        );

        return (
            <ModalTrigger
                triggerNode={this.props.triggerNode}
                className='popup-modal-edit-dashboard'
                isButton
                modalTitle={modalTitle}
                modalIcon={modalIcon}
                modalBody={modalContent}
                modalFooter={modalFooter}
            />
        );
    }
}

DashboardEdit.propTypes = propTypes;

export default DashboardEdit;
