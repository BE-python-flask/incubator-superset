import React from 'react';
import Controls from './Controls';
import { Tooltip } from 'antd';

const propTypes = {
    dashboard: React.PropTypes.object,
};
const defaultProps = {
};

class Header extends React.PureComponent {
    constructor(props) {
        super(props);
        this.state = {
        };
    }
    render() {
        const dashboard = this.props.dashboard;
        return (
            <div className="title">
                <div className="pull-left">
                    <span
                        className="name text-overflow-style"
                        style={{maxWidth: 260, display: 'inline-block'}}
                    >
                        {dashboard.name}
                    </span>
                    <span is class="favstar" class_name="Dashboard" obj_id={dashboard.id} />
                    <Tooltip title={dashboard.description} placement="bottom">
                        <i className="icon icon-info"/>
                    </Tooltip>
                </div>
                <div className="pull-right">
                    {!this.props.dashboard.context.standalone_mode &&
                    <Controls dashboard={dashboard} />
                    }
                </div>
                <div className="clearfix" />
            </div>
        );
    }
}
Header.propTypes = propTypes;
Header.defaultProps = defaultProps;

export default Header;
