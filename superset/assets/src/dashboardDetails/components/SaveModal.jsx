const $ = window.$ = require('jquery');

import React from 'react';
import ReactDOM from 'react-dom';
import {render} from 'react-dom';
import {Button, FormControl, FormGroup, Radio} from 'react-bootstrap';
import {getAjaxErrorMsg} from '../../modules/utils';
import {PILOT_PREFIX, renderLoadingModal} from '../../utils/utils';
import ModalTrigger from '../../components/ModalTrigger';
import {ConfirmModal} from '../../common/components';
import intl from 'react-intl-universal';

const propTypes = {
    css: React.PropTypes.string,
    dashboard: React.PropTypes.object.isRequired,
    triggerNode: React.PropTypes.node.isRequired,
};

class SaveModal extends React.PureComponent {
    constructor(props) {
        super(props);
        this.state = {
            dashboard: props.dashboard,
            css: props.css,
            saveType: 'overwrite',
            newDashName: '',
        };
        this.modal = null;
        this.handleSaveTypeChange = this.handleSaveTypeChange.bind(this);
        this.handleNameChange = this.handleNameChange.bind(this);
        this.saveDashboard = this.saveDashboard.bind(this);
    }
    handleSaveTypeChange(event) {
        this.setState({
            saveType: event.target.value,
        });
    }
    handleNameChange(event) {
        this.setState({
            newDashName: event.target.value,
            saveType: 'newDashboard',
        });
    }
    saveDashboardRequest(data, url, saveType) {
        const dashboard = this.props.dashboard;
        const saveModal = this.modal;
        const loadingModal = renderLoadingModal();
        loadingModal.show();
        $.ajax({
            type: 'POST',
            url,
            data: {
                data: JSON.stringify(data),
            },
            success(resp) {
                saveModal.close();
                dashboard.onSave();
                loadingModal.hide();
                if (saveType === 'newDashboard') {
                    window.location = PILOT_PREFIX + 'dashboard/' + $.parseJSON(resp.data).id + '/';
                } else {
                    window.location.reload();
                }
            },
            error(error) {
                saveModal.close();
                loadingModal.hide();
                const errorMsg = getAjaxErrorMsg(error);
                const confirmMessage = intl.get('DASHBOARD.SAVE_ERROR') +'  '+ errorMsg;
                render(
                    <ConfirmModal
                        confirmType='error'
                        confirmMessage={confirmMessage}
                    />,
                    document.getElementById('popup_root')
                );
            },
        });
    }
    saveDashboard(saveType, newDashboardTitle) {
        const dashboard = this.props.dashboard;
        const expandedSlices = {};
        $.each($('.slice_info'), function () {
            const widget = $(this).parents('.widget');
            const sliceDescription = widget.find('.slice_description');
            if (sliceDescription.is(':visible')) {
                expandedSlices[$(widget).attr('data-slice-id')] = true;
            }
        });
        const positions = dashboard.reactGridLayout.serialize();
        const data = {
            positions,
            css: this.state.css,
            expanded_slices: expandedSlices,
        };
        let url = null;
        if (saveType === 'overwrite') {
            url = PILOT_PREFIX + 'save_dash/' + dashboard.id + '/';
            this.saveDashboardRequest(data, url, saveType);
        } else if (saveType === 'newDashboard') {
            if (!newDashboardTitle) {
                this.modal.close();
                render(
                    <ConfirmModal
                        confirmType='error'
                        confirmMessage={intl.get('DASHBOARD.SELECT_NAME_FOR_NEW_DASHBOARD')}
                    />,
                    document.getElementById('popup_root')
                );
            } else {
                data.name = newDashboardTitle;
                url = PILOT_PREFIX + 'copy_dash/' + dashboard.id + '/';
                this.saveDashboardRequest(data, url, saveType);
            }
        }
    }

    render() {
        return (
            <ModalTrigger
                ref={(modal) => { this.modal = modal; }}
                triggerNode={this.props.triggerNode}
                isButton
                modalTitle={intl.get('DASHBOARD.SAVE_DASHBOARD')}
                modalIcon="icon icon-save"
                className="popup-modal-save-dashboard"
                modalBody={
                    <FormGroup>
                        <Radio
                            value="overwrite"
                            onChange={this.handleSaveTypeChange}
                            checked={this.state.saveType === 'overwrite'}>
                            {intl.get('DASHBOARD.OVERRIDE_DASHBOARD')} [{this.props.dashboard.name}]
                        </Radio>
                        <Radio
                            value="newDashboard"
                            onChange={this.handleSaveTypeChange}
                            checked={this.state.saveType === 'newDashboard'}>
                            {intl.get('DASHBOARD.SAVE_AS')}:
                        </Radio>
                        <FormControl
                            type="text"
                            placeholder={intl.get('DASHBOARD.DASHBOARD_NAME')}
                            onFocus={this.handleNameChange}
                            onChange={this.handleNameChange}
                        />
                    </FormGroup>
                }
                modalFooter={
                    <div>
                        <Button
                            type="button"
                            className="tp-btn tp-btn-middle tp-btn-primary"
                            data-dismiss="modal"
                            onClick={() => { this.saveDashboard(this.state.saveType, this.state.newDashName); }}
                            >
                            {intl.get('DASHBOARD.SAVE')}
                        </Button>
                    </div>
                }
            />
        );
    }
}
SaveModal.propTypes = propTypes;

export default SaveModal;
