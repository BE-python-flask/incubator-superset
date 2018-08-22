import React from 'react';
import { render } from 'react-dom';
import { createStore, applyMiddleware } from 'redux';
import { Provider } from 'react-redux';
import { Main } from './databaseList/containers';
import configureStore from './databaseList/stores/configureStore';

import './databaseList/style/database.scss';
import { replaceAppName } from './utils/utils.jsx';

replaceAppName();
const store = configureStore();
const rootElement = document.querySelector('#databaseList');
const $ = window.$ = require('jquery');
const jQuery = window.jQuery = require('jquery'); // eslint-disable-line
require('bootstrap');

// $('.nav > li:nth-child(4)').addClass('active');

render(
    <Provider store={store}>
        <Main />
    </Provider>,
    rootElement
);
