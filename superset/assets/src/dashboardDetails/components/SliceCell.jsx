import React from 'react';
import PropTypes from 'prop-types';
import { Tooltip } from 'antd';

const propTypes = {
    slice: PropTypes.object.isRequired,
    removeSlice: PropTypes.func.isRequired,
    expandedSlices: PropTypes.object,
};

function SliceCell({ expandedSlices, removeSlice, slice, intl }) {
    return (
        <div className="slice-cell" id={`${slice.token}-cell`}>
            <div className="chart-header">
                <div className="header-title">
                    <Tooltip title={slice.slice_name} placement="top">
                        <span>{slice.slice_name}</span>
                    </Tooltip>
                </div>
                <div className="header-controls">
                    <Tooltip title={intl.get('DASHBOARD.DRAG_SLICE')} placement="top">
                        <a><i className="fa fa-arrows drag"/></a>
                    </Tooltip>
                    <Tooltip title={intl.get('DASHBOARD.FORCE_REFRESH')} placement="top">
                        <a className="refresh"><i className="fa fa-repeat"/></a>
                    </Tooltip>
                    {slice.description &&
                    <Tooltip title={slice.description} placement="top">
                        <a><i className="fa fa-info-circle slice_info"/></a>
                    </Tooltip>
                    }
                    <Tooltip title={intl.get('DASHBOARD.EDIT_DASHBOARD')} placement="top">
                        <a href={slice.slice_url}><i className="fa fa-pencil"/></a>
                    </Tooltip>
                    <Tooltip title={intl.get('DASHBOARD.REMOVE_SLICE')} placement="top">
                        <a className="remove-chart"><i className="fa fa-close" onClick={() => { removeSlice(slice.slice_id); }}/></a>
                    </Tooltip>
                </div>
            </div>
            <div
                className="slice_description bs-callout bs-callout-default"
                style={
                    expandedSlices &&
                    expandedSlices[String(slice.slice_id)] ? {} : { display: 'none' }
                }
                dangerouslySetInnerHTML={{ __html: slice.description_markeddown }}
            >
            </div>
            <div className="row chart-container">
                <input type="hidden" value="false" />
                <div id={slice.token} className="token col-md-12">
                    <img
                        src="/static/assets/images/loading.gif"
                        className="loading"
                        alt="loading"
                    />
                    <div className="slice_container" id={slice.token + '_con'}></div>
                </div>
            </div>
        </div>
    );
}

SliceCell.propTypes = propTypes;

export default SliceCell;
