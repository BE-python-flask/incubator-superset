import React from 'react';
import PropTypes from 'prop-types';

import './ErrorAlert.scss'

const ErrorAlert = ({message}) => {
    return (
        <div className="error-alert">
            <div className="error-sign">
                <div className="sign-container">
                    <div className="line-1"/>
                    <div className="line-2"/>
                </div>
            </div>
            <div className="error-content">
                <h5>Error</h5>
                <pre>{message}</pre>
            </div>
        </div>
    );
};

ErrorAlert.propTypes = {};

export default ErrorAlert;