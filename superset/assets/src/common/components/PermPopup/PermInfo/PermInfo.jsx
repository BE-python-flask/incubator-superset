import React from 'react';
import PropTypes from 'prop-types';
import { Tooltip } from 'antd';

import './PermInfo.scss';

const PermInfo = ({infos}) => {
    const permInfos = infos.map((info, index) => {
        return <Tooltip key={index} placement="topLeft" title={info}>
                <pre className="info-item">{info}</pre>
            </Tooltip>
    });
    return (
        <div className="perm-info" >
            {permInfos}
        </div>
    );
};

PermInfo.propTypes = {};

export default PermInfo;