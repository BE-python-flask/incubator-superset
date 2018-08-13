import React, { Component } from 'react';
import { Link } from 'react-router-dom';

import { bindActionCreators } from 'redux';
import { connect } from 'react-redux';
import PropTypes from 'prop-types';
import * as actions from '../actions';
import { CONSTANT } from '../actions';
import * as utils from './../../utils/utils';
import intl from "react-intl-universal";

import '../style/file-browser.scss';

class FileBrowser extends Component {
    constructor(props) {
        super(props);
        this.state = {
            initDone: true
        };
    }

    componentDidMount() {
        this.loadLocales();
        this.props.fetchPreview();
    }

    loadLocales() {
        utils.loadIntlResources(_ => {
            this.setState({ initDone: true });
        });
    }

    componentWillReceiveProps(nextProps) {
        const {emitFetch} = nextProps;

        if (emitFetch.isFetching !== this.props.emitFetch.isFetching) {
            const loadingModal = utils.renderLoadingModal();
            if (emitFetch.isFetching) {
                loadingModal.show();
            } else {
                loadingModal.hide();
            }
        }
    }

    download(ag) {
        this.props.fetchDownload();
    }

    refresh(ag) {
        this.props.fetchPreview();
    }

    render() {
        const {fileReducer, changePath, condition} = this.props;
        const {path, mtime, size, user, group, mode, preview} = fileReducer;
        const linkToPath = (ag) => {
            this.setState({
                ...ag
            });
            changePath(ag);
        }

        if (this.state.initDone) {
            return (
                <div className="file-browse">
                    <div className="title-bar">
                        <i className="icon icon-edit-slice edit-slice-ps"></i>
                        <span>{intl.get('file_browser')}</span>
                    </div>
                    <div className="content">
                        <div className="item-left fleft">
                            <div className="item-left-module operate">
                                <h3>{intl.get('action')}</h3>
                                <ul className="module-list">
                                    { /*<li><span>以文本格式查看</span><i className="icon icon-text-hover"></i></li>*/ }
                                    <li onClick={(_ => this.download(_))} >
                                        <span>{intl.get('download')}</span>
                                        <a>
                                            <i className="icon icon-download-hover"></i>
                                        </a>
                                    </li>
                                    { /*<li><span>查看文件位置</span><i className="icon icon-file-hover"></i></li>*/ }
                                    <li onClick={(_ => this.refresh(_))} >
                                        <span>{intl.get('refresh')}</span>
                                        <i className="icon icon-refresh-hover"></i>
                                    </li>
                                </ul>
                            </div>
                            <div className="item-left-module infor">
                                <h3>{intl.get('information')}</h3>
                                <ul className="module-list">
                                    <li><span><i className="icon icon-time-default"></i>{intl.get('latest_change')}</span><em>{mtime}</em></li>
                                    <li><span><i className="icon icon-user-default"></i>{intl.get('users')}</span><em>{user}</em></li>
                                    <li><span><i className="icon icon-group-default"></i>{intl.get('group')}</span><em>{group}</em></li>
                                    <li><span><i className="icon icon-storage－default"></i>{intl.get('size')}</span><em>{size}</em></li>
                                    <li><span><i className="icon icon-pattern-default"></i>{intl.get('pattern')}</span><em>{mode}</em></li>
                                </ul>
                            </div>
                        </div>

                        <div className="item-right">
                            <div className="bread-crumb sec-class"
                style={{ }}
                >
                                <span className="f16">
                                    <Link to="/">{intl.get('previous')}</Link>
                                </span>
                                <textarea rows="1"
                className={'f16 path'}
                name="pathName"
                value={path}
                onChange={_=>_}
                >
                                </textarea>
                            </div>

                            <div className="tableWrapper">
                            {preview}
                            </div>
                        </div>
                    </div>
                </div>
            );
        }else{
            return <div></div>;
        }
    }
}

FileBrowser.propTypes = {};

function mapStateToProps(state) {
    const {fileReducer, condition, emitFetch} = state;

    return {
        fileReducer,
        condition,
        emitFetch
    };
}

function mapDispatchToProps(dispatch) {

    const {fetchPreview, changePath, fetchDownload} = bindActionCreators(actions, dispatch);

    return {
        fetchPreview,
        changePath,
        fetchDownload
    };

}

export default connect(
    mapStateToProps,
    mapDispatchToProps
)(FileBrowser);
