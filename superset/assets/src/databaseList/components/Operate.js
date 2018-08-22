import React from 'react';
import {render} from 'react-dom';

import {Link}  from 'react-router-dom';
import {ConnectionDelete, ConnectionAdd} from '../popup';
import PropTypes from 'prop-types';
import * as actions from '../actions';
import {Select}  from './';
import intl from 'react-intl-universal';
import {OperationSelect} from '../../common/components';
import {transformObjectToArray} from '../utils';
import {renderGlobalErrorMsg} from '../../utils/utils.jsx';

class Operate extends React.Component {
    constructor(props, context) {
        super(props);
        this.state = {
            DBTypes: []
        };

        this.onChange = this.onChange.bind(this);
        this.onEnterSearch = this.onEnterSearch.bind(this);
        this.onDelete = this.onDelete.bind(this);
        this.onAdd = this.onAdd.bind(this);
        this.onSearch = this.onSearch.bind(this);

        this.dispatch = context.dispatch;
        this.timer = null;
    }

    componentDidMount () {
        this.fetchTypes();
    }

    fetchTypes () {
        const self = this;
        const callback = (success, data) => {
            if(success) {
                const types = [];
                data.map((obj, index) => {
                    types.push({id: index+1, label: obj});
                });
                self.setState({DBTypes: types});
            }else {
                renderGlobalErrorMsg(data);
            }
        };

        self.dispatch(actions.fetchTypes(callback));
    }

    onChange () {
        const dispatch = this.dispatch;
        if( this.refs.searchField.value ){
            this.refs.searchIcon.removeAttribute('disabled');
        } else {
            this.refs.searchIcon.setAttribute('disabled', 'disabled');
            this.timer && clearTimeout(this.timer);
            this.timer = setTimeout(function(){
                dispatch(actions.search(''));
            }, 300);
        }
    }

    onAdd (type) {
        const dispatch = this.dispatch;
        render(
            <ConnectionAdd
                dispatch={dispatch}
                connectionType={type}
            />,
            document.getElementById('popup_root')
        );
    }

    //multi delete
    onDelete () {
        const dispatch = this.dispatch;
        const { selectedRowNames } = this.props;
        dispatch(actions.fetchConnectDelMulInfo(callback));
        function callback(success, data) {
            if(success) {
                let deleteType;
                let deleteTips = data;
                if(selectedRowNames.length === 0) {
                    deleteType = 'none';
                    deleteTips = intl.get('DATABASE.NO_SELECT_DELETE_TIP');
                }
                render(
                    <ConnectionDelete
                        dispatch={dispatch}
                        deleteTips={deleteTips}
                        deleteType={deleteType}
                    />,
                    document.getElementById('popup_root')
                );
            }else {
                message(data, 5);
            }
        }
    }

    onSearch () {
        const filter = this.refs.searchField.value;
        this.dispatch(actions.search(filter));
    }

    onEnterSearch(event) {
        if(event.keyCode === 13) {
            this.onSearch();
        }
    }

    render () {
        const DBTypes = this.state.DBTypes;
        const typeArray = transformObjectToArray(DBTypes, 'label');
        return (
            <div className="operations">
                <ul className="icon-list">
                    <li
                        style={{width:'130px', textAlign:'left'}}
                    >
                        <OperationSelect
                            opeType='addConnect'
                            iconClass='icon icon-plus'
                            options={typeArray}
                            selectChange={(argus)=>this.onAdd(argus)}
                        >
                        </OperationSelect>
                    </li>
                    <li onClick={this.onDelete}>
                        <i className="icon icon-trash"/>
                    </li>
                </ul>
                <div className="search-input" style={{ marginRight: 0 }}>
                    <input
                        onKeyUp={this.onEnterSearch}
                        onChange={this.onChange}
                        className="tp-input"
                        ref="searchField"
                        placeholder={intl.get('DATABASE.SEARCH')}
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

Operate.propTypes = {};
Operate.contextTypes = {
    dispatch: PropTypes.func.isRequired
};
export default Operate;