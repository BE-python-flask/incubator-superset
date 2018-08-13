import React from 'react';
import ReactDOM from 'react-dom';
import { Select, Checkbox, Tooltip, Alert } from 'antd';
import PropTypes from 'prop-types';
const _ = require('lodash');
import intl from 'react-intl-universal';

class TableColumnAdd extends React.Component {
    constructor(props) {
        super(props);
        this.state = {
            enableConfirm: false,
            tableColumn: {
                column_name: '',
                filterable: false,
                count_distinct: false,
                expression: '',
                description: '',
                max: false,
                sum: false,
                avg: false,
                groupby: false,
                dataset_id: '',
                is_dttm: false,
                min: false,
                type: ''
            },
            exception: {}
        };

        // bindings
        this.confirm = this.confirm.bind(this);
        this.closeDialog = this.closeDialog.bind(this);

        this.handleCheckboxChange = this.handleCheckboxChange.bind(this);
        this.handleInputChange = this.handleInputChange.bind(this);
    };

    componentDidMount() {
        let data = {
            tableColumn: this.state.tableColumn
        };

        // 编辑
        if (this.props.editedColumn) {
            data.tableColumn = this.props.editedColumn;
        }
        else {
            data.tableColumn.dataset_id = this.props.datasetId;
        }

        this.setState(data);
    }

    closeDialog() {
        ReactDOM.unmountComponentAtNode(document.getElementById("popup_root"));
    }

    handleCheckboxChange(e) {
        let data = {
            tableColumn: this.state.tableColumn
        };
        data.tableColumn[e.target.name] = e.target.checked;
        this.setState(data);
    }

    handleInputChange(e) {
        let data = {
            tableColumn: this.state.tableColumn
        };
        data.tableColumn[e.target.name] = e.target.value;
        this.setState(data);
        this.formValidate();
    }

    formValidate () {
        const tc = this.state.tableColumn;
        if (tc.column_name && tc.expression && tc.type && tc.dataset_id) {
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

    confirm() {
        const self = this;
        const { fetchTableColumnAdd, fetchTableColumnEdit } = self.props;
        if (fetchTableColumnAdd) {
            fetchTableColumnAdd(self.state.tableColumn, callback);
        }
        else {
            fetchTableColumnEdit(self.state.tableColumn, callback);
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

    render() {

        const CheckboxGroup = Checkbox.Group;
        const checkboxOptions = [
            {
                label: intl.get('DATASET.GROUP'),
                value: 'groupby'
            },
            {
                label: intl.get('DATASEET.FILTER'),
                value: 'filterable'
            },
            {
                label: intl.get('DATASEET.COUNT'),
                value: 'count_distinct'
            },
            {
                label: intl.get('DATASEET.SHOW_TIME'),
                value: 'is_dttm'
            },
            {
                label: intl.get('DATASEET.MIN'),
                value: 'min'
            },
            {
                label: intl.get('DATASEET.MAX'),
                value: 'max'
            },
            {
                label: intl.get('DATASEET.SUM'),
                value: 'sum'
            },
            {
                label: intl.get('DATASEET.ADVANTAGE'),
                value: 'avg'
            }
        ];
        const column = this.state.tableColumn;

        const {
            groupby,
            filterable,
            count_distinct,
            sum,
            avg,
            min,
            max,
            is_dttm
        } = column;

        return (
            <div className="popup">
                <div className="popup-dialog popup-md">
                    <div className="popup-content">
                        <div className="popup-header">
                            <div className="header-left">
                                <i className="icon icon-dataset"/>
                                <span className="item-label">{this.props.title}</span>
                            </div>
                            <div className="header-right">
                                <i className="icon icon-close" onClick={this.closeDialog}/>
                            </div>
                        </div>
                        <div className="popup-body">
                            <div className="dialog-item">
                                <div className="item-left">
                                    <i>*</i>
                                    <span className="item-label">{intl.get('DATASET.COLUMN')}：</span>
                                </div>
                                <div className="item-right">
                                    <input
                                        className="tp-input dialog-input"
                                        name="column_name"
                                        value={column.column_name}
                                        onChange={this.handleInputChange}
                                    />
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
                                        value={column.expression}
                                        onChange={this.handleInputChange}
                                    />
                                    <Tooltip placement="topRight" title={intl.get('DATASET.COLUMN_EXPRESSION_TIP')}>
                                        <i
                                            className="icon icon-info after-textarea-icon"
                                            style={{top: 35}}
                                        />
                                    </Tooltip>
                                </div>
                            </div>
                            <div className="dialog-item">
                                <div className="item-left">
                                    <i>*</i>
                                    <span className="item-label">{intl.get('DATASET.TYPE')}：</span>
                                </div>
                                <div className="item-right">
                                    <input
                                        className="tp-input dialog-input"
                                        name="type"
                                        value={column.type}
                                        onChange={this.handleInputChange}
                                    />
                                </div>
                            </div>
                            <div className="dialog-item">
                                <div className="item-left"></div>
                                <div className="item-right">
                                    <Checkbox name="groupby" checked={groupby} onChange={this.handleCheckboxChange}>{intl.get('DATASET.GROUP')}</Checkbox>
                                    <Checkbox name="filterable" checked={filterable} onChange={this.handleCheckboxChange}>{intl.get('DATASET.FILTER')}</Checkbox>
                                    <Checkbox name="count_distinct" checked={count_distinct} onChange={this.handleCheckboxChange}>{intl.get('DATASET.COUNT')}</Checkbox>
                                    <Checkbox name="is_dttm" checked={is_dttm} onChange={this.handleCheckboxChange}>{intl.get('DATASET.SHOW_TIME')}</Checkbox>
                                    <Checkbox name="sum" checked={sum} onChange={this.handleCheckboxChange}>{intl.get('DATASET.SUM')}</Checkbox>
                                    <Checkbox name="avg" checked={avg} onChange={this.handleCheckboxChange}>{intl.get('DATASET.AVERAGE')}</Checkbox>
                                    <Checkbox name="min" checked={min} onChange={this.handleCheckboxChange}>{intl.get('DATASET.MIN')}</Checkbox>
                                    <Checkbox name="max" checked={max} onChange={this.handleCheckboxChange}>{intl.get('DATASET.MAX')}</Checkbox>
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

TableColumnAdd.propTypes = propTypes;
TableColumnAdd.defaultProps = defaultProps;

export default TableColumnAdd;