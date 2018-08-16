import React from 'react';
import ReactDOM from 'react-dom';
import PropTypes from 'prop-types';
import { WarningAlert } from '../../common/components/WarningAlert';
import intl from 'react-intl-universal';

class TableColumnDelete extends React.Component {
    constructor(props) {
        super(props);
        this.state = {};
        // bindings
        this.confirm = this.confirm.bind(this);
        this.closeDialog = this.closeDialog.bind(this);
    };

    closeDialog() {
        ReactDOM.unmountComponentAtNode(document.getElementById("popup_root"));
    }

    confirm() {
        const self = this;
        const { fetchTableColumnDelete, column } = self.props;
        fetchTableColumnDelete(column.id, callback);

        function callback(success) {
            if(success) {
                self.closeDialog();
            }else {

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
                                <i className="icon icon-trash"/>
                                <span>{intl.get('DATASET.COLUMN_ATTRIBUTE')}</span>
                            </div>
                            <div className="header-right">
                                <i
                                    className="icon icon-close"
                                    onClick={this.closeDialog}
                                />
                            </div>
                        </div>
                        <div className="popup-body">
                            <WarningAlert
                                message={this.props.deleteTips}
                            />
                        </div>
                        <div className="popup-footer">
                            <button
                                className="tp-btn tp-btn-middle tp-btn-primary"
                                onClick={this.confirm}
                            >
                                {intl.get('DATASET.CONFIRM')}
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

TableColumnDelete.propTypes = propTypes;
TableColumnDelete.defaultProps = defaultProps;

export default TableColumnDelete;
