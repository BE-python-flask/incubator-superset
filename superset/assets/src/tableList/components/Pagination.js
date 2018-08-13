import React from 'react';
import ReactDOM from 'react-dom';
import PropTypes from 'prop-types';
import { navigateTo, changePageSize} from '../actions';
import { Pagination, LocaleProvider } from 'antd';
import enUS from 'antd/lib/locale-provider/en_US';
import zhCN from 'antd/lib/locale-provider/zh_CN';
import { getAntdLocale } from '../../../utils/utils';

class SlicePagination extends React.Component {
  constructor(props) {
    super(props);
    this.state = {}
  }

  render() {
      const { dispatch, pageSize, count, pageNumber} = this.props;

      function onChange(page) {
          dispatch(navigateTo(page));
      }

      function onShowSizeChange(current, size) {
          dispatch(changePageSize(size));
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

SlicePagination.propTypes = {};

export default SlicePagination;