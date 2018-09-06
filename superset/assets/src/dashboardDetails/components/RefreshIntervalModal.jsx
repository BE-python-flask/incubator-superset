import React from 'react';

import ModalTrigger from '../../components/ModalTrigger';
import Select from 'react-select';

const propTypes = {
    triggerNode: React.PropTypes.node.isRequired,
    initialRefreshFrequency: React.PropTypes.number,
    onChange: React.PropTypes.func,
};

const defaultProps = {
    initialRefreshFrequency: 0,
    onChange: () => {},
};

const options = [
    [0, "不刷新"],
    [10, '10 秒'],
    [30, '30 秒'],
    [60, '1 分钟'],
    [300, '5 分钟'],
].map(o => ({ value: o[0], label: o[1] }));

class RefreshIntervalModal extends React.PureComponent {
    constructor(props) {
        super(props);
        this.state = {
            refreshFrequency: props.initialRefreshFrequency,
        };
    }
    render() {
        return (
            <ModalTrigger
                triggerNode={this.props.triggerNode}
                isButton
                modalTitle="刷新间隔"
                modalIcon="icon icon-clock"
                modalBody={
                    <div>
                        选择仪表盘的刷新频率
                        <Select
                          options={options}
                          value={this.state.refreshFrequency}
                          onChange={(opt) => {
                            this.setState({ refreshFrequency: opt.value });
                            this.props.onChange(opt.value);
                          }}
                        />
                    </div>
                }
            />
        );
    }
}
RefreshIntervalModal.propTypes = propTypes;
RefreshIntervalModal.defaultProps = defaultProps;

export default RefreshIntervalModal;
