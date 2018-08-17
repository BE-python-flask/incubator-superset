import React from 'react';
import { render } from 'react-dom';
import { Provider } from 'react-redux';
import { HashRouter, Route, Link, IndexRoute } from 'react-router-dom';
import configureStore from './home/store/configureStore';
import { Home, EditDetail, EventDetail } from './home/containers';
import { replaceAppName } from './utils/utils';
// import '../stylesheets/scss/_home.scss'

const $ = window.$ = require('jquery');
const jQuery = window.jQuery = require('jquery'); // eslint-disable-line
require('bootstrap');

const _ = require('lodash');

const store = configureStore();

// $('.nav > li:nth-child(1)').addClass('active');
replaceAppName();
render(
  <Provider store={store}>
    <HashRouter>
      <div>
        <Route exact path="/" component={Home} />
        <Route path="/editDetail" component={EditDetail} />
        <Route path="/eventDetail" component={EventDetail} />
      </div>
    </HashRouter>
  </Provider>,
  document.querySelector('#home')
);
