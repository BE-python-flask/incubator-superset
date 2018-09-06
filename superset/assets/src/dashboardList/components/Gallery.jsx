import React from 'react';
import PropTypes from 'prop-types';
import { GalleryItem } from '../components';

class Gallery extends React.Component {
    constructor(props) {
        super(props);
        this.state = {};
        // bindings
    };

    render() {
        const { dispatch, dashboardList, selectedRowKeys, selectedRowNames } = this.props;
        let items = [];
        if(dashboardList) {
            dashboardList.map((dashboard) => {
                const item = <GalleryItem
                    key={dashboard.id}
                    dispatch={dispatch}
                    dashboard={dashboard}
                    selectedRowKeys={selectedRowKeys}
                    selectedRowNames={selectedRowNames}/>;
                items.push(item);
            });
        }
        return (
            <div className="gallery">
                {items}
            </div>
        );
    }
}

const propTypes = {};
const defaultProps = {};

Gallery.propTypes = propTypes;
Gallery.defaultProps = defaultProps;

export default Gallery;