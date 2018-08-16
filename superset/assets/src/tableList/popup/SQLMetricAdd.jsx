import React from 'react';
import ReactDOM from 'react-dom';
import { Select, Checkbox, Tooltip, Alert } from 'antd';
import PropTypes from 'prop-types';
import { getMetricTypeOptions } from '../module';
import intl from 'react-intl-universal';

class SQLMetricAdd extends React.Component {
    constructor(props) {
        super(props);
        this.state = {
            enableConfirm: false,
            metric: {
                metric_name: "",
                expression: "",
                metric_type: "",
                dataset_id: ""
            },
            exception: {},
            options: []
        };
        // bindings
        this.confirm = this.confirm.bind(this);
        this.closeDialog = this.closeDialog.bind(this);
        this.handleInputChange = this.handleInputChange.bind(this);
        this.handleSelectChange = this.handleSelectChange.bind(this);
    };

    componentDidMount() {
        let data = {
            metric: this.state.metric
        };
        
        if (this.props.editedMetric) {
            data.metric = this.props.editedMetric;
        }
        else {
            data.metric.dataset_id = this.props.datasetId;
        }

        this.initTypeOptions();
        this.setState(data);
    }

    initTypeOptions() {
        const options = getMetricTypeOptions();
        this.setState({
            options: options
        });
    }

    closeDialog() {
        ReactDOM.unmountComponentAtNode(document.getElementById("popup_root"));
    }


    confirm() {
        const self = this;
        const { fetchSQLMetricAdd, fetchSQLMetricEdit } = self.props;
        if (fetchSQLMetricAdd) {
            fetchSQLMetricAdd(self.state.metric, callback);
        }
        else {
            fetchSQLMetricEdit(self.state.metric, callback);
        }

        function callback(success, message) {
            if(success) {
                self.closeDialog();
            }else {
                self.refs.alertRef.style.display = "block";
                let exception = {};
                exception.type = "error";
                exception.message = "Error";
                exception.description = message;
                self.setState({
                    exception: exception
                });
            }
        }
    }

    handleInputChange(e) {
        let data = {
            metric: this.state.metric
        };
        data.metric[e.target.name] = e.target.value;
        this.setState(data);
        this.formValidate();
    }

    handleSelectChange(type) {
        let data = {
            metric: this.state.metric
        };
        data.metric.metric_type = type;
        this.setState(data);
        this.formValidate();
    }

    formValidate() {
        const mt = this.state.metric;
        if (mt.metric_name && mt.expression && mt.metric_type && mt.dataset_id) {
            this.setState({
                enableConfirm: true
            });
        }
        else {
            this.setState({
                enableConfirm: false
            });
        }
    }

    render() {

        const metric = this.state.metric;
        return (
            <div className="popup">
                <div className="popup-dialog popup-md">
                    <div className="popup-content" id="addSqlMetric">
                        <div className="popup-header">
                            <div className="header-left">
                                <i className="icon icon-dataset"/>
                                <span className="item-label">
                                    {this.props.title}
                                </span>
                            </div>
                            <div className="header-right">
                                <i
                                    className="icon icon-close"
                                    onClick={this.closeDialog}
                                />
                            </div>
                        </div>
                        <div className="popup-body">
                            <div className="dialog-item">
                                <div className="item-left">
                                    <i>*</i>
                                    <span className="item-label">{intl.get('DATASET.METRIC')}：</span>
                                </div>
                                <div className="item-right">
                                    <input
                                        className="tp-input dialog-input"
                                        value={metric.metric_name}
                                        name="metric_name"
                                        onChange={this.handleInputChange}
                                    />
                                </div>
                            </div>
                            <div className="dialog-item">
                                <div className="item-left">
                                    <i>*</i>
                                    <span className="item-label">{intl.get('DATASET.TYPE')}：</span>
                                </div>
                                <div className="item-right">
                                    <Select
                                        style={{ width: '100%' }}
                                        onSelect={this.handleSelectChange}
                                        getPopupContainer={() => document.getElementById('addSqlMetric')}
                                    >
                                        {this.state.options}
                                    </Select>
                                    <Tooltip
                                        placement="topRight"
                                        title={intl.get('DATASET.METRIC_TYPE_TIP')}
                                    >
                                        <i className="icon icon-info after-icon"/>
                                    </Tooltip>
                                </div>
                            </div>
                            <div className="dialog-item">
                                <div className="item-left">
                                    <i>*</i>
                                    <span className="item-label">{intl.get('DATASET.EXPRESSION')}：</span>
                                </div>
                                <div className="item-right">
                                    <textarea
                                        className="tp-textarea dialog-area"
                                        name="expression"
                                        value={metric.expression}
                                        onChange={this.handleInputChange}
                                    />
                                    <Tooltip
                                        placement="topRight"
                                        title={intl.get('DATASET.METRIC_EXPRESSION_TIP')}
                                    >
                                        <i
                                            className="icon icon-info after-icon"
                                            style={{top: 30}}
                                        />
                                    </Tooltip>
                                </div>
                            </div>
                            <div className="error" ref="alertRef" style={{display: 'none'}}>
                                <Alert
                                    message={this.state.exception.message}
                                    description={this.state.exception.description}
                                    type={this.state.exception.type}
                                    closeText="close"
                                    showIcon
                                />
                            </div>
                        </div>
                        <div className="popup-footer">
                            <button
                                className="tp-btn tp-btn-middle tp-btn-primary"
                                onClick={this.confirm}
                                    disabled={!this.state.enableConfirm}
                            >
                                {intl.get('DATASET.SAVE')}
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

SQLMetricAdd.propTypes = propTypes;
SQLMetricAdd.defaultProps = defaultProps;

export default SQLMetricAdd;