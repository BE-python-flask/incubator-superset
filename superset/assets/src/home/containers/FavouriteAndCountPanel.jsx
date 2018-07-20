import React, { Component } from 'react';
import PropTypes from 'prop-types';
import {connect} from "react-redux";
import { createSelector } from 'reselect';
import  { Bar } from "../components";
import { swithTabInFavourite } from "../actions";
import intl from "react-intl-universal";

const _ = require('lodash');

class FavouriteAndCountPanel extends Component {
    constructor(props) {
        super();
    }

    render() {
        let selected = this.props.currentCatagory || "dashboard";
        const { onChangeCatagory, favbarData, refBarData } = this.props;

        return (
            <aside className="top10-and-worktimes">
                <div className="top10 white-bg-and-border-radius">
                    <div className="index-title-module">
                        <h3>{intl.get('favorite_top_10')}</h3>
                        <div className="title-tab">
                            <ul>
                                <li onClick={ () => {onChangeCatagory('dashboard')} } className={selected==='slice'?'':'current'}>{intl.get('dashboard')}</li>
                                <li onClick={ () => {onChangeCatagory('slice')} } className={selected==='slice'?'current':''}>{intl.get('slice')}</li>
                            </ul>
                        </div>
                        <div className="transparent"></div>
                    </div>
                    <div className="times-barchart" style={{background:'transparent'}}>
                        <Bar barData={favbarData}></Bar>
                    </div>
                </div>
                <div className="worktimes white-bg-and-border-radius">
                    <div className="index-title-module">
                        <h3>{intl.get('recommend_top_10')}</h3>
                    </div>
                    <div className="times-barchart" style={{background:'transparent'}}>
                        <Bar barData={refBarData}></Bar>
                    </div>
                </div>
            </aside>
        );
    }
}

const getFavBarData = createSelector(
    state => state.posts.param.favorits,
    state => state.switcher.favoritePanelCatagory,
    (data, catagory) => {
        let barData = {
            series: {
                name: intl.get('amount'),
                data: []
            },
            categories: [],
            urls: []
        };

        if (!data) {
            return barData;
        }

        let array = [];
        if (catagory === 'dashboard') {
            array = data['dashboard']
        }
        else {
            array = data['slice'];
        }
        _.forEach(array, (obj, key) => {
            barData.series.data.push(obj.count);
            barData.categories.push(obj.name);
            barData.urls.push(obj.link);
        });

        return barData;
    }
);

const getRefBarData = createSelector(
    state => state.posts.param.refers,
    (data) => {
        let barData = {
            series: {
                name: intl.get('amount'),
                data: []
            },
            categories: [],
            urls: []
        }

        if (!data) {
            return barData;
        }

        _.forEach(data, (obj, key) => {
            barData.series.data.push(obj.count);
            barData.categories.push(obj.name);
            barData.urls.push(obj.link);
        });

        return barData;
    }
);

FavouriteAndCountPanel.propTypes = {
    currentCatagory: PropTypes.string.isRequired,
    favbarData: PropTypes.object,
    refBarData: PropTypes.object
}

const mapStateToProps = (state) => {
    const { switcher } = state;
    return {
        currentCatagory: switcher.favoritePanelCatagory,
        favbarData: getFavBarData(state),
        refBarData: getRefBarData(state)
    }
}

const mapDispatchToProps = (dispatch) => {
    return {
        onChangeCatagory: (catagory) => {
            dispatch(swithTabInFavourite(catagory));
        }
    }
}

export default connect(mapStateToProps, mapDispatchToProps)(FavouriteAndCountPanel);