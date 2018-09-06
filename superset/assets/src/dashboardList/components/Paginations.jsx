import React from 'react';
import { render } from 'react-dom';
import PropTypes from 'prop-types';
import * as actions from '../actions';
import { Pagination, LocaleProvider } from 'antd';
import enUS from 'antd/lib/locale-provider/en_US';
import zhCN from 'antd/lib/locale-provider/zh_CN';
import { getAntdLocale } from '../../utils/utils';

class Paginations extends React.Component {
    constructor(props) {
        super(props);
        this.state = {};
        // bindings
    };

    render() {

        const { dispatch, pageSize, count, pageNumber} = this.props;

        function onChange(page) {
            dispatch(actions.setPageNumber(page));
            dispatch(actions.fetchPosts());
        }

        function onShowSizeChange(current, size) {
            dispatch(actions.setPageSize(size));
            dispatch(actions.fetchPosts());
        }

        return (
            <LocaleProvider locale={getAntdLocale(zhCN,enUS)}>
                <Pagination
                    showQuickJumper
                    showSizeChanger
                    pageSize={pageSize}
                    total={count}
                    current={pageNumber}
                    onChange={onChange}
                    onShowSizeChange={onShowSizeChange}
                />
            </LocaleProvider>
        );
    }
}

const propTypes = {};
const defaultProps = {};

Paginations.propTypes = propTypes;
Paginations.defaultProps = defaultProps;

export default Paginations;
