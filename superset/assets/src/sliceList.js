import React from 'react';
import { render } from 'react-dom';
import { createStore, applyMiddleware } from 'redux';
import { Provider } from 'react-redux';
import thunk from 'redux-thunk';
import App from './sliceList/containers/SliceConnection';
import configureStore from './sliceList/stores/configureStore';
import 'antd/dist/antd.css';
import { replaceAppName } from './utils/utils.jsx';
replaceAppName();

const $ = window.$ = require('jquery');
const jQuery = window.jQuery = require('jquery'); // eslint-disable-line
require('bootstrap');

const store = configureStore();
const rootElement = document.querySelector('#slices');
// $('.nav > li:nth-child(3)').addClass('active');

render(
  <Provider store={store}>
    <App />
  </Provider>,
  rootElement
);
