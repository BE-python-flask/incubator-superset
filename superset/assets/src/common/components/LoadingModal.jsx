import React from 'react';
import ReactDOM from 'react-dom';
import PropTypes from 'prop-types';
require('../../../stylesheets/loading.css');
class LoadingModal extends React.Component {
    constructor(props) {
        super(props);
        this.state = {};
        // bindings
        this.hide = this.hide.bind(this);
        this.show = this.show.bind(this);
    };

    show() {
        this.refs.loading.style.display = "flex";
    }

    hide() {
        ReactDOM.unmountComponentAtNode(
            document.getElementById("loading_root")
        );
    }

    render() {
        return (
            <div className="loading-overlay" ref="loading">
                <div className="transwarp-spinner-img"></div>
                <div className="loading-image"></div>
            </div>
        );
    }
}

const propTypes = {};
const defaultProps = {};

LoadingModal.propTypes = propTypes;
LoadingModal.defaultProps = defaultProps;

export default LoadingModal;