/**
 * Created by haitao on 17-5-15.
 */
import React from 'react';
import { render } from 'react-dom';
import { createStore, applyMiddleware } from 'redux';
import { Provider } from 'react-redux';
import thunk from 'redux-thunk';
import { HashRouter as Router, Route } from 'react-router-dom'
import TableContainer from './dashboardList/containers/TableContainer';
import GraphContainer from './dashboardList/containers/GraphContainer';
import configureStore from './dashboardList/store/configureStore';
import { replaceAppName } from './utils/utils.jsx';

const $ = window.$ = require('jquery');
const jQuery = window.jQuery = require('jquery'); // eslint-disable-line
require('bootstrap');
const store = configureStore();

// $('.nav > li:nth-child(2)').addClass('active');

replaceAppName();
$(document).ready(() => {
    render(
        <Provider store={store}>
            <Router>
                <div>
                    <Route exact path="/" component={TableContainer} />
                </div>
            </Router>
        </Provider>,
        document.getElementById('dashboard')
    );
});
