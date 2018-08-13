import React from 'react';
import { render } from 'react-dom';
import { bindActionCreators } from 'redux';
import { connect } from 'react-redux';

import PropTypes from 'prop-types';
import * as actions from '../actions';
import { CONSTANT } from '../actions';

import { Select, PopupNormal } from './';
import { OperationSelect } from '../../common/components';
import { getPermData, updatePermMode, getPopupType } from '../module';
import intl from "react-intl-universal";

const _ = require('lodash');

class Operate extends React.Component {
    constructor(props, context) {
        super(props);
        this.state = {};
        this.dispatch = context.dispatch;

        this.manipulateOptions = [];
        this.createOptions = [];

        this.searchOnChange = this.searchOnChange.bind(this);
        this.onRemove = this.onRemove.bind(this);
        this.onSearch = this.onSearch.bind(this);
        this.upload = this.upload.bind(this);
        this.handleSelectChange = this.handleSelectChange.bind(this);
    }

    componentDidMount() {
        this.manipulateOptions = [
            {
                id: CONSTANT.move,
                name: intl.get('move')
            },
            {
                id: CONSTANT.copy,
                name: intl.get('copy')
            },
            {
                id: CONSTANT.download,
                name: intl.get('download')
            },
            {
                id: CONSTANT.auth,
                name: intl.get('chang_right')
            }
        ];
        this.createOptions = [
            {
                id: CONSTANT.mkdir,
                name: intl.get('catalog')
            },
            {
                id: CONSTANT.touch,
                name: intl.get('files')
            }
        ];
    }

    cleanNormalParamState(popupNormalParam) {
        let obj = {
            ...popupNormalParam
        };
        for (let key in obj) {
            if (obj.hasOwnProperty(key)) {
                obj[key] = '';
            }
        }
        obj['alertStatus'] = 'none';
        obj['treeData'] = [];
        obj['permData'] = [];
        obj['recursivePerm'] = false;
        obj['showAlert'] = false;
        obj['disabled'] = 'disabled';
        return obj;
    }

    manipulate(ag) {
        const popupType = getPopupType(ag, this.manipulateOptions.concat(this.createOptions));
        const {fetchOperation, popupNormalParam, setPopupNormalParams, setPermData, setPermMode, setHDFSPath, fetchHDFSList, condition} = this.props;
        let obj = this.cleanNormalParamState(popupNormalParam);

        let normalPopupParam = {};
        if (condition.selectedRows.length !== 0 || _.indexOf([CONSTANT.mkdir, CONSTANT.touch], popupType) >= 0) {
            if (popupType === CONSTANT.auth || popupType === CONSTANT.copy || popupType === CONSTANT.move || popupType === CONSTANT.download) {
                normalPopupParam = {
                    ...obj,
                    popupType: popupType,
                    submit: fetchOperation,
                    status: 'flex'
                };
                setPopupNormalParams(normalPopupParam);
                if (popupType === CONSTANT.auth) {
                    const permData = getPermData(condition.selectedRows);
                    const permMode = updatePermMode(permData);
                    setPermData(permData);
                    setPermMode(permMode);
                    setHDFSPath(condition.selectedRows[0].path);
                } else if (popupType === CONSTANT.copy || popupType === CONSTANT.move) {
                    fetchHDFSList('/', true);
                }
            } else if (popupType === CONSTANT.mkdir || popupType === CONSTANT.touch) {
                normalPopupParam = {
                    ...obj,
                    path: condition.path,
                    popupType: popupType,
                    submit: fetchOperation,
                    status: 'flex'
                };
                setPopupNormalParams(normalPopupParam);
            }
        } else {
            normalPopupParam = {
                ...obj,
                popupType: CONSTANT.noSelect,
                submit: fetchOperation,
                status: 'flex'
            };
            setPopupNormalParams(normalPopupParam);
        }
    }

    searchOnChange() {
        const me = this;
        const dispatch = me.dispatch;
        const {emitFetch, swapResponse, returnResponse} = me.props;
        const {response, heap} = emitFetch;
        let value = me.refs.searchField.value;
        let data = {
            ...heap
        };

        if (this.refs.searchField.value) {
            this.refs.searchIcon.removeAttribute('disabled');
            //TODO: get the data from heap
            data.files = data.files.filter((element, index, array) => {
                let bu = element.name.indexOf(value) >= 0;
                return bu;
            });
            dispatch(swapResponse({
                response: data
            }));
        } else {
            this.refs.searchIcon.setAttribute('disabled', 'disabled');
            dispatch(swapResponse({
                response: heap
            }));
        }

    }

    upload() {
        const {fetchOperation, popupNormalParam, setPopupNormalParams, condition} = this.props;

        let obj = this.cleanNormalParamState(popupNormalParam);
        let normalPopupParam = {
            ...obj,
            popupType: CONSTANT.upload,
            submit: fetchOperation,
            status: 'flex',
            dest_path: condition.path
        };
        setPopupNormalParams(normalPopupParam);
    }

    onRemove() {
        const {fetchOperation, popupNormalParam, setPopupNormalParams, condition} = this.props;
        if (condition.selectedRowNames.length === 0) {
            const normalPopupParam = this.cleanNormalParamState(popupNormalParam);
            Object.assign(normalPopupParam, {
                popupType: CONSTANT.noSelect,
                submit: fetchOperation,
            });
            setPopupNormalParams(normalPopupParam);
            return;
        }

        let deleteTips = intl.get('sure_to_delete') + condition.selectedRowNames.join(' ') + '?';
        let normalPopupParam = {
            ...popupNormalParam,
            popupType: CONSTANT.remove,
            submit: fetchOperation,
            status: 'flex',
            deleteTips: deleteTips
        };
        setPopupNormalParams(normalPopupParam);
    }

    handleSelectChange(argus) {
        this.dispatch(selectType(argus));
    }

    onSearch() {
        const filter = this.refs.searchField.value;
        this.props.search(filter);
    }

    render() {
        return (
            <div className="operations">
                <div className="popupContainer">
                    <PopupNormal />
                </div>

                <ul className="icon-list">
                    <li
            className="li-setting"
            >
                        <OperationSelect
            opeType="hdfsOperation"
            iconClass="icon icon-setting ps"
            options={this.manipulateOptions}
            placeholder={intl.get('edit')}
            selectChange={(argus) => this.manipulate(argus)}
            >
                        </OperationSelect>
                    </li>
                    <li
            className="li-upload"
            onClick={this.upload}
            >
                        <i className="icon icon-upload ps"/>{intl.get('upload')}
                    </li>
                    <li
            className="li-plus"
            >
                        <OperationSelect
            opeType="addFolder"
            iconClass="icon icon-plus ps"
            options={this.createOptions}
            placeholder={intl.get('add')}
            selectChange={(argus) => this.manipulate(argus)}
            >
                        </OperationSelect>
                    </li>
                    <li
            className="li-trash  bolder-right-none"
            onClick={this.onRemove}
            >
                        <i className="icon icon-trash ps"/>{intl.get('delete')}
                    </li>
                </ul>
                <div className="search-input" style={{
                marginRight: 0
            }}>
                    <input
            onChange={this.searchOnChange}
            ref="searchField"
            className="tp-input"
            placeholder={intl.get('search_ellipsis')} />
                    <i className="icon icon-search" onClick={this.onSearch} ref="searchIcon"/>
                </div>
            </div>
        );
    }
}

Operate.propTypes = {};
Operate.contextTypes = {
    dispatch: PropTypes.func.isRequired
};

function mapStateToProps(state, pros) {
    const {condition, popupNormalParam, emitFetch} = state;

    return {
        condition,
        popupNormalParam,
        emitFetch
    };
}

export default connect(mapStateToProps, actions)(Operate);