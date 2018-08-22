import React from 'react';
import ReactDOM from 'react-dom';
import { fetchAddDashboard, setDashAddConfirmState } from '../../../dashboard2/actions';

import { Select } from 'antd';
import PropTypes from 'prop-types';

class Add extends React.Component {
    constructor(props) {
        super(props);
        this.state = {
            selectedSlices: [],
            enableConfirm: false
        };
        // bindings
        this.confirm = this.confirm.bind(this);
        this.closeDialog = this.closeDialog.bind(this);
        this.showDialog = this.showDialog.bind(this);

        this.handleTitleChange = this.handleTitleChange.bind(this);
        this.handleDescriptionChange = this.handleDescriptionChange.bind(this);
    };

    showDialog() {
        this.refs.popupDashboardAdd.style.display = "flex";
    }

    closeDialog() {
        this.setState({
            dashboard: {},
            selectedSlices: [],
            enableConfirm: false
        });
        this.refs.popupDashboardAdd.style.display = "none";
    }

    handleTitleChange(e) {
        this.props.dashboard.name = e.currentTarget.value;
        this.setState({
            dashboard: this.props.dashboard
        });
        if(!e.currentTarget.value || e.currentTarget.value.length === 0) {
            this.setState({
                enableConfirm: false
            });
        }else {
            this.setState({
                enableConfirm: true
            });
        }
    }

    handleDescriptionChange(e) {
        this.props.dashboard.description = e.target.value;
        this.setState({
            dashboard: this.props.dashboard
        });
    }

    confirm() {
        const self = this;
        const { dispatch, availableSlices } = self.props;
        dispatch(fetchAddDashboard(self.state, availableSlices, callback));
        function callback(success) {
            if(success) {
                self.setState({
                    dashboard: {},
                    selectedSlices: [],
                    enableConfirm: false
                });
                self.refs.popupDashboardAdd.style.display = "none";
            }else {

            }
        }
    }

    componentDidMount() {

    }

    render() {
        const self = this;
        const Option = Select.Option;
        const options = self.props.availableSlices.map(d => {
            return <Option key={d.id}>{d.slice_name}</Option>
        });

        function onChange(value) {
            self.setState({
                selectedSlices: value
            });
        }

        return (
            <div className="popup" ref="popupDashboardAdd">
                <div className="popup-dialog popup-md">
                    <div className="popup-content">
                        <div className="popup-header">
                            <div className="header-left">
                                <i className="icon"></i>
                                <span>仪表盘基本信息</span>
                            </div>
                            <div className="header-right">
                                <i className="icon" onClick={this.closeDialog}></i>
                            </div>
                        </div>
                        <div className="popup-body">
                            <div className="dialog-item">
                                <div className="item-left">
                                    <span>标题：</span>
                                </div>
                                <div className="item-right">
                                    <input className="form-control dialog-input" value={this.props.dashboard.name}
                                           onChange={this.handleTitleChange} />
                                </div>
                            </div>
                            <div className="dialog-item">
                                <div className="item-left">
                                    <span>描述：</span>
                                </div>
                                <div className="item-right">
                                    <textarea className="dialog-area" value={this.props.dashboard.description}
                                              onChange={this.handleDescriptionChange}></textarea>
                                </div>
                            </div>
                            <div className="dialog-item">
                                <div className="item-left">
                                    <span>工作表：</span>
                                </div>
                                <div className="item-right">
                                    <div id="edit_pop_select">
                                        <Select mode={'multiple'}
                                                value={self.state.selectedSlices}
                                                style={{ width: '100%' }}
                                                placeholder="select the slices..."
                                                onChange={onChange}>
                                            {options}
                                        </Select>
                                    </div>
                                </div>
                            </div>
                            <div className="dialog-item">
                                <div className="item-left">
                                    <span>数据集：</span>
                                </div>
                                <div className="item-right">
                                    <input className="form-control dialog-input" disabled />
                                </div>
                            </div>
                        </div>
                        <div className="popup-footer">
                            <button className="tp-btn tp-btn-middle tp-btn-primary" onClick={this.confirm}
                                    disabled={!this.state.enableConfirm}>
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

Add.propTypes = propTypes;
Add.defaultProps = defaultProps;

export default Add;