import React, { Component } from 'react';
import PropTypes from 'prop-types';
import {connect} from "react-redux";
import { createSelector } from 'reselect';
import { Link } from 'react-router-dom';
import { EditList, EventList } from "../components";
import { swithTabInEdit } from "../actions";
import intl from "react-intl-universal";

const _ = require('lodash');

class EditAndEventPanel extends Component {
    constructor(props) {
        super();
    }

    render() {

        let selected = this.props.currentCatagory || "dashboard";
        const { onChangeCatagory, editList, eventList } = this.props;

        return (
            <aside className="recentedit-and-event">
                <div className="recentedit white-bg-and-border-radius">
                    <div className="index-title-module">
                        <h3>{intl.get('recent_edited')}</h3>
                        <div className="title-tab">
                            <ul onClick={ () => {} }>
                                <li onClick={ () => {onChangeCatagory('dashboard')} } className={selected==='slice'?'':'current'}>{intl.get('dashboard')}</li>
                                <li onClick={ () => {onChangeCatagory('slice')} } className={selected==='slice'?'current':''}>{intl.get('slice')}</li>
                            </ul>
                        </div>
                        <div className="more">
                            <Link to="/editDetail">
                                <i className="icon more-icon"></i>
                            </Link>
                        </div>
                    </div>
                    <div className="edit-list">
                        {<EditList {...editList} catagory={selected} />}
                    </div>
                </div>
                <div className="event white-bg-and-border-radius">
                    <div className="index-title-module">
                        <h3>{intl.get('events')}</h3>
                        <div className="more">
                            <Link to="/eventDetail">
                                <i className="icon more-icon"></i>
                            </Link>
                        </div>
                    </div>
                    <div className="event-list">
                        {<EventList eventList={eventList} />}
                    </div>
                </div>
            </aside>
        );
    }
}

const getEidtListData = createSelector(
    state => state.posts.param.edits,
    (data) => {
        if (!data) {
            return {};
        }

        let result = {};
        let item =  {};
        let dataArr = [];
        _.forEach(data, (arr, key) => {
            result[key] = {};
            dataArr = [];
            _.forEach( arr, (obj, key) => {
                item = {
                    'key': key + 1,
                    'name': obj.name,
                    'action': obj.action,
                    'time': obj.time,
                    'link': obj.link
                };
                dataArr.push(item);
            });
            result[key] = dataArr;
        });

        return result;
    }
);

const getEventListData = createSelector(
    state => state.posts.param.actions,
    (data) => {
        if (!data) {
            return [];
        }

        let result = [];
        let item = {};
        _.forEach(data, (obj, key) => {
            item = {
                'key': key + 1,
                'user': obj.user,
                'action': obj.action,
                'time': obj.time,
                'link': obj.link,
                'title': obj.title,
                'type': obj.obj_type
            };
            result.push(item);
        });

        return result;
    }
);

EditAndEventPanel.propTypes = {
    currentCatagory: PropTypes.string.isRequired,
    editList: PropTypes.object,
    eventList: PropTypes.array
}


const mapStateToProps = (state) => {
    const { switcher, posts } = state;
    return {
        currentCatagory: switcher.editPanelCatagory,
        editList: getEidtListData(state),
        eventList: getEventListData(state)
    }
}

const mapDispatchToProps = (dispatch) => {
    return {
        onChangeCatagory: (catagory) => {
            dispatch(swithTabInEdit(catagory));
        }
    }
}

export default connect(mapStateToProps, mapDispatchToProps)(EditAndEventPanel);