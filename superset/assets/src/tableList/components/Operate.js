import React from 'react';
import {render} from 'react-dom';
import {Link}  from 'react-router-dom';
import {Select} from 'antd';
import {OperationSelect} from '../../common/components';
import PropTypes from 'prop-types';
import * as actions from '../actions';
import {TableDelete} from '../popup';
import intl from 'react-intl-universal';
import {renderGlobalErrorMsg} from '../../../utils/utils';

class SliceOperate extends React.Component {
    constructor(props) {
        super(props);
        this.state = {
            addDatasetTypes: [],
            filterDatasetTypes: []
        };

        this.onChange = this.onChange.bind(this);
        this.onDelete = this.onDelete.bind(this);
        this.onSearch = this.onSearch.bind(this);
        this.onEnterSearch = this.onEnterSearch.bind(this);
        this.selectChange = this.selectChange.bind(this);
        this.handleSelectChange = this.handleSelectChange.bind(this);
    }

    onChange () {
        const { dispatch } = this.props;
        if( this.refs.searchField.value ){
            this.refs.searchIcon.removeAttribute('disabled');
        } else {
            this.refs.searchIcon.setAttribute('disabled', 'disabled');
        }
        if(this.refs.searchField.value === "") {
            this.onSearch();
        }
    }

    onDelete () {
        const { dispatch, selectedRowNames } = this.props;
        dispatch(actions.fetchTableDelMulInfo(callback));
        function callback(success, data) {
            if(success) {
                let deleteType = 'multiple';
                let deleteTips = data;
                if(selectedRowNames.length === 0) {
                    deleteType = 'none';
                    deleteTips = intl.get('DATASET.NO_SELECT_DELETE_TIP');
                }
                render(
                    <TableDelete
                        dispatch={dispatch}
                        deleteType={deleteType}
                        deleteTips={deleteTips} />,
                    document.getElementById('popup_root')
                );
            }else {
                renderGlobalErrorMsg(data);
            }
        }
    }

    selectChange(value) {
        const { dispatch, saveDatasetId, clearDatasetData } = this.props;
        dispatch(saveDatasetId(''));
        dispatch(clearDatasetData());
        dispatch(actions.switchDatasetType(value));
    }

    handleSelectChange (argus) {
        this.props.dispatch(actions.selectType(argus));
    }

    onSearch () {
        const filter = this.refs.searchField.value;
        this.props.dispatch(actions.search(filter));
    }

    onEnterSearch(event) {
        if(event.keyCode === 13) {
            this.onSearch();
        }
    }

    componentDidMount() {
        const self = this;
        const { dispatch } = self.props;
        dispatch(actions.fetchAddTypeList(addCallback));
        dispatch(actions.fetchFilterTypeList(filterCallback));
        function addCallback(success, data) {
            if(success) {
                self.setState({
                    addDatasetTypes: data
                });
            }else {
                renderGlobalErrorMsg(data);
            }
        }
        function filterCallback(success, data) {
            if(success) {
                self.setState({
                    filterDatasetTypes: data
                });
            }else {
                renderGlobalErrorMsg(data);
            }
        }
    }

    render() {
        return (
            <div className="operations">
                <ul className="icon-list">
                    <li style={{ width: '130px', textAlign: 'left' }}>
                        <OperationSelect
                            opeType='addDataset'
                            iconClass='icon icon-plus'
                            options={this.state.addDatasetTypes}
                            selectChange={this.selectChange}
                        >
                        </OperationSelect>
                    </li>
                    <li onClick={this.onDelete}>
                        <i className="icon icon-trash" />
                    </li>
                </ul>
                <ul className="icon-list" style={{ marginLeft: 0 }}>
                    <li style={{ width: '130px' }}>
                        <OperationSelect
                            opeType='filterDataset'
                            iconClass='icon icon-filter'
                            defaultValue='ALL'
                            options={this.state.filterDatasetTypes}
                            selectChange={this.handleSelectChange}
                        >
                        </OperationSelect>
                    </li>
                </ul>
                <div className="search-input" style={{marginRight: 0}}>
                    <input
                        onKeyUp={this.onEnterSearch}
                        onChange={this.onChange}
                        className="tp-input"
                        ref="searchField"
                        placeholder={intl.get('DATASET.SEARCH')}
                    />
                    <i
                        className="icon icon-search"
                        onClick={this.onSearch}
                        ref="searchIcon"
                    />
                </div>
            </div>
        );
    }
}

SliceOperate.propTypes = {};

export default SliceOperate;