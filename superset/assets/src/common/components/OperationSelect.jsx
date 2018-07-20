/**
 * Created by haitao on 17-7-17.
 */
import React from 'react';
import { render } from 'react-dom';
import PropTypes from 'prop-types';
import { Link }  from 'react-router-dom';
import intl from 'react-intl-universal';
import { getEleOffsetLeft, getEleOffsetTop, loadIntlResources } from '../../utils/utils';

const $ = window.$ = require('jquery');

class OperationSelect extends React.Component {
    constructor(props) {
        super(props);
        this.state = {
            opened: false,
            selected: props.defaultValue
        };
        this.onToggle = this.onToggle.bind(this);
    }

    componentDidMount() {
        loadIntlResources(_ => this.setState({
            initDone: true
        }));
    }

    onToggle(event) {
        event.stopPropagation();
        let opened = this.state.opened;
        const opeType = this.props.opeType;
        this.setState({
            opened: !opened
        });

        const self = this;
        let flag = true;
        $(document).bind("click",function(e){
            let targetEl = document.getElementById(opeType);
            if(targetEl) {
                let x = e.clientX;
                let y = e.clientY;
                let minX = getEleOffsetLeft(targetEl);
                let minY = getEleOffsetTop(targetEl);
                let maxX = minX + targetEl.offsetWidth;
                let maxY = minY + targetEl.offsetHeight;
                if(flag && (x<minX || x>maxX || y<minY || y>maxY)){
                    self.setState({
                        opened: false
                    });
                    return;
                }
            }
        });
    }

    onSelect(self) {
        self.setState({
            selected: this,
            opened: false
        });
        self.props.selectChange(this);
    }

    render () {
        const self = this;
        const { options, opeType, iconClass, placeholder } = self.props;
        let typeOptions = [];
        if(opeType === "addDataset") {
            typeOptions = options.map((opt, index) => {
                let opted = opt;
                if(opt === 'UPLOAD FILE') {
                    opted = 'UPLOAD_FILE';
                }
                return <li className={this.state.selected===opt?'selected':''}
                           key={index} onClick={self.onSelect.bind(opted, self)}>
                    <Link to={`/add/detail/${opted}/`}>{opt}</Link>
                </li>
            });
        }else if(opeType === "addConnect") {
            typeOptions = options.map((opt, index) => {
                return <li className={this.state.selected===opt?'selected':''}
                           key={index} onClick={self.onSelect.bind(opt, self)}>
                    <span>{opt}</span>
                </li>
            });
        }else if(opeType === "hdfsOperation" || opeType === "addFolder") {
            typeOptions = options.map((opt, index) => {
                return <li className={this.state.selected===opt?'selected':''}
                           key={opt.id} onClick={self.onSelect.bind(opt.name, self)}>
                    <span>{opt.name}</span>
                </li>
            });
        }else if(opeType === "filterDataset") {
            typeOptions = options.map((opt, index) => {
                return <li className={this.state.selected===opt?'selected':''}
                           key={opt} onClick={self.onSelect.bind(opt, self)}>
                    <span>{opt}</span>
                </li>
            });
        }
        
        return (
            <div className="common-select">
                <div id={opeType} className="selection-toggle" onClick={this.onToggle}>
                    <i className={iconClass}/>
                    <span>{this.state.selected ||  placeholder || intl.get('please_select')}</span>
                    <i className={this.state.opened?'icon icon-open-sign':'icon icon-close-sign'}/>
                </div>
                <div className={this.state.opened===true?'selection-section':'none'}>
                    <ul>{typeOptions}</ul>
                </div>
            </div>
        );
    }
}

OperationSelect.propTypes = {};

export default OperationSelect;