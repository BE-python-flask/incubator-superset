import React from 'react';
import { render } from 'react-dom';
import { createStore, applyMiddleware } from 'redux';
import { Provider } from 'react-redux';
import { Main, FileBrowser } from './hdfsList/containers';
import configureStore from './hdfsList/stores/configureStore';
import { Route, HashRouter } from 'react-router-dom';
import './hdfsList/style/hdfs.scss';
import { replaceAppName } from './utils/utils.jsx';

replaceAppName();
const $ = window.$ = require('jquery');
const jQuery = window.jQuery = require('jquery'); // eslint-disable-line
require('bootstrap');

const store = configureStore();
const rootElement = document.querySelector('#hdfsList');
// $('.nav > li:nth-child(6)').addClass('active');

render(
    <Provider store={store}>
        <HashRouter>
            <div>
                <Route exact path="/" component={Main} />
                <Route path="/filebrowser" component={FileBrowser} />
            </div>
        </HashRouter>
    </Provider>,
    rootElement
);

