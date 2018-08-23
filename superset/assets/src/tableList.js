import React from 'react';
import { render } from 'react-dom';
import { createStore, applyMiddleware } from 'redux';
import { Provider } from 'react-redux';
import thunk from 'redux-thunk';
import App from './tableList/containers/';
import { Main, TableAdd, } from './tableList/containers';
import configureStore from './tableList/stores/configureStore';
import { HashRouter, BrowserRouter, Route, IndexRoute } from 'react-router-dom';
import { replaceAppName } from './utils/utils.jsx';
replaceAppName();
const store = configureStore();
const rootElement = document.querySelector('#tableList');

const $ = window.$ = require('jquery');
const jQuery = window.jQuery = require('jquery'); // eslint-disable-line
require('bootstrap');

$('.nav > li:nth-child(4)').addClass('active');

render(
    <Provider store={store}>
        <HashRouter>
            <div>
                <Route exact path="/" component={Main} />
                <Route path="/add" component={TableAdd} />
                <Route path="/edit" component={TableAdd} />
            </div>
        </HashRouter>
    </Provider>,
    rootElement
);
