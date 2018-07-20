import React from 'react';
import PropTypes from 'prop-types';

import './WarningAlert.scss'

const WarningAlert = ({message}) => {
    return (
        <div className="warning-alert">
            <div className="warning-sign">
                <div className="sign-container">
                    <div className="symbol">
                        <div className="vertical-line"/>
                        <div className="dot"/>
                    </div>
                </div>
            </div>
            <div className="warning-content">
                <h5>Warning</h5>
                <pre>{message}</pre>
            </div>
        </div>
    );
};

WarningAlert.propTypes = {};

export default WarningAlert;