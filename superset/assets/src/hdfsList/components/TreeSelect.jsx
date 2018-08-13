import React, { Component } from 'react';
import { TreeSelect } from 'antd';
import PropTypes from 'prop-types';

class TreeSelector extends Component {

    constructor(props, context) {
        super(props);
        this.onSelect = this.onSelect.bind(this);
        this.onLoadData = this.onLoadData.bind(this);
    }

    onSelect(value, node)  {
        if(node.props.isLeaf) {
            return;
        }
        const { setPopupNormalParams, popupNormalParam } = this.props;
        setPopupNormalParams({
            ...popupNormalParam,
            dest_path: node.props.value,
            treeVal: value
        });
    }

    onLoadData(node) {
        const { fetchHDFSList } = this.props;
        const hdfsPath = node.props.value;
        return fetchHDFSList(hdfsPath, false);
    }

    render() {

        const {treeData, treeVal} = this.props;
        return (
            <TreeSelect
                value={treeVal}
                allowClear
                showSearch
                style={{
                    width: 420
                }}
                dropdownStyle={{
                    maxHeight: 300,
                    overflow: 'auto'
                }}
                treeData={treeData}
                onSelect={this.onSelect}
                loadData={this.onLoadData}
                placeholder="please select dest-path"
                getPopupContainer={() => document.getElementById('hdfs-tree-select')}
            />
        );
    }
}

TreeSelector.contextTypes = {
};

export default TreeSelector;