import React from 'react';
import ReactHighcharts from 'react-highcharts';
import PropTypes from 'prop-types';

import intl from "react-intl-universal";

const _ = require('lodash');

function addLink(categories, urls) {
  let categoryLinks = {};
  _.forEach(categories, function(category, index) {
    categoryLinks[category] = urls[index];
  });
  return categoryLinks;
}

function makeDummy(data, max) {
  var dummy = [];
  _.forEach(data, function(point, i) {
    dummy.push(max - point);
  });
  return dummy;
}

function Bar(props) {
  if (!props.barData) {
    return <p> { props.intl.get('faulty_data') } </p>;
  }

  const { categories, series, urls } = props.barData || {
    categories: [],
    series: {
      name: "",
      data: []
    },
    urls: []
  };

  let height = 30 * categories.length + 30;

  let maxs = [];
  maxs.push(Math.max.apply(null, series.data));
  let max = Math.round(Math.max.apply(null, maxs) * 1.1);
  let dummyData = makeDummy(series.data, max);
  let categoryLinks = addLink(categories, urls);

  const config = {
    chart: {
      type: 'bar',
      height: height
    },
    title: {
      text: ''
    },
    lang: {
      noData: intl.get('no_data')
    },
    xAxis: {
      categories: categories,
      title: {
        text: null
      },
      lineWidth: 0,
      minorGridLineWidth: 0,
      lineColor: 'transparent',
      minorTickLength: 0,
      tickLength: 0,
      labels: {
        style: {
          color: '#7c8ca5',
          fontSize: 14,
          fontWeight: 'bold',
          whiteSpace: 'nowrap',
          overflow: "hidden",
          textOverflow: 'ellipsis',
          maxWidth: "120px",
          fontFamily: "'Microsoft YaHei', 'Helvetica Neue', Helvetica, Arial, 'PingFang SC', 'Hiragino Sans GB', 'Heiti SC', 'WenQuanYi Micro Hei', sans-serif",
        },
        formatter: function() {
          return '<a href="' + categoryLinks[this.value] + '" class="bar-item-link">' +
            this.value + '</a>';
        },
        useHTML: true
      }
    },
    yAxis: {
      min: 0,
      max: max,
      title: {
        text: '',
        align: 'high'
      },
      visible: false
    },
    plotOptions: {
      series: {
        dataLabels: {
          enabled: true,
          align: 'right',
          color: '#FFFFFF',
          x: -10
        },
        pointPadding: 0,
        groupPadding: 0,
        stacking: 'normal',
        borderRadius: 4,
        pointWidth: 24
      }
    },
    tooltip: {
      valueSuffix: '',
      enabled: false
    },
    legend: {
      enabled: false
    },
    credits: {
      enabled: false
    },
    noData: {
      style: {
        fontWeight: 'bold',
        fontSize: '15px',
        color: '#303030'
      }
    },
    series: [{
        stack: 'a',
        showInLegend: false,
        enableMouseTracking: false,
        name: 'Fill Series',
        color: '#f5f8fa',
        data: dummyData,
        pointWidth: 24,
        dataLabels: {
          enabled: false
        }
      },
      {
        stack: 'a',
        name: series.name,
        data: series.data,
        pointWidth: 24,
        color: '#2ea1f8'
      }
    ]
  };


  return ( <ReactHighcharts config = { config } > </ReactHighcharts>
  );
}

Bar.propTypes = {
  barData: PropTypes.object
};

export default Bar;