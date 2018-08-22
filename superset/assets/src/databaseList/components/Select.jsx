import React from 'react';
import { Select } from 'antd';
const Option = Select.Option;
import PropTypes from 'prop-types';

class ConnectionNameSelect extends React.Component {

    constructor (props) {
        super(props);
        this.state = {};

        this.handleSelect = this.handleSelect.bind(this);
    }

    handleSelect (valueObj) {
        this.props.handleSelect(valueObj);
    }

    render () {

        const {options, showText, width, value} = this.props;

        const children = options.map((obj, key) =>
            <Option key={obj.id}>{obj.label}</Option>
        );

        return (
            <div>
                <Select
                    defaultValue={value || 'select database name'}
                    style={{width: width}}
                    onSelect={this.handleSelect}
                >

                    {children}
                </Select>
            </div>
        );
    }
}

export default ConnectionNameSelect;