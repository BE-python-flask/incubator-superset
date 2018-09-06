const $ = window.$ = require('jquery');
import React from 'react';
import { render, ReactDOM } from 'react-dom';
import { ButtonGroup } from 'react-bootstrap';
import Button from '../../components/Button';
import RefreshIntervalModal from './RefreshIntervalModal';
import SaveModal from './SaveModal';
import CodeModal from './CodeModal';
import SliceAdder from './SliceAdder';
import DashboardEdit from './DashboardEdit';
import { ConfirmModal } from '../../common/components';
import intl from 'react-intl-universal';
import { renderLoadingModal, loadIntlResources } from '../../utils/utils';

const propTypes = {
    dashboard: React.PropTypes.object.isRequired,
};

class Controls extends React.PureComponent {
    constructor(props) {
        super(props);
        this.state = {
            css: props.dashboard.css,
            initDone: false
        };
    }
    refresh() {
        this.props.dashboard.sliceObjects.forEach((slice) => {
            slice.render(true);
        });
    }

    componentDidMount() {
        loadIntlResources(_ => this.setState({ initDone: true }), 'dashboard');
    }

    render() {
        const dashboard = this.props.dashboard;
        const canSave = dashboard.context.dash_save_perm; //cannot use currently
        return (
            <ButtonGroup>
                <Button
                    onClick={this.refresh.bind(this)}
                    tooltip={intl.get('DASHBOARD.REFRESH_DASHBOARD')}
                    placement="bottom"
                    >
                    <i className="icon icon-refresh"/>
                </Button>
                <SliceAdder
                    dashboard={dashboard}
                    triggerNode={
                        <i className="icon icon-plus"/>
                    }
                />
                <DashboardEdit
                    dashboard={dashboard}
                    triggerNode={
                        <i className="icon icon-edit"/>
                    }
                />
                <SaveModal
                    dashboard={dashboard}
                    css={this.state.css}
                    triggerNode={
                        <i className="icon icon-save"/>
                    }
                />
            </ButtonGroup>
        );
    }
}
Controls.propTypes = propTypes;

export default Controls;
