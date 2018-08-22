import React from 'react';
//import { IndexRoute } from 'react-router';
import { Route, IndexRoute } from 'react-router-dom';

import {
    Main,
    DatabaseAdd,
//        SubDetail,
//        SubPreview,
//        SubColumns,
//        SubSqlMetric
    } from './containers';

export default () => (
    <div>
        <Route exact path="/" component={Main} />
        <Route path="/add" component={DatabaseAdd} />
    </div>
);