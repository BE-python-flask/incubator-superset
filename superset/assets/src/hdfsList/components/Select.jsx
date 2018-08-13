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

        const {options, theValue, showText, width} = this.props;

        const children = options.map((obj, key) => {
            return <Option key={obj.id} className={obj.name}>{obj.name}</Option>
        });

        return (
            <div>
                <Select
                    labelInValue
                    defaultValue={{ key: theValue||'select from connections'}}
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