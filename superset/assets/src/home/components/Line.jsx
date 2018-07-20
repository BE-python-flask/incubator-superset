import React from 'react';
import ReactHighcharts from 'react-highcharts';
import PropTypes from 'prop-types';
import Highcharts from 'highcharts';
import intl from "react-intl-universal";

function Line(props) {

    if (!props.statistic) return <p>{intl.get('faulty_data')}</p>;

    const {catagories, series, catagoriesWithOutYear} = props.statistic.chart || {
        catagories: [],
        series: []
    };
    const title = props.title;

    const config = {
        chart: {
            // backgroundColor: 'rgba(46, 161, 248, 0.1)',
            type: 'area',
            height: '250'
        },
        lang: {
            noData: intl.get('no_data')
        },
        credits: {
            enabled: false
        },
        title: {
            text: '',
            align: "left"
        },
        noData: {
            style: {
                fontWeight: 'bold',
                fontSize: '15px',
                color: '#303030'
            }
        },
        yAxis: {
            title: {
                text: ''
            }
        },
        plotOptions: {
            series: {
                color: '#1991eb',
                fillOpacity: 0.1,
                marker: {
                    fillColor: '#FFFFFF',
                    lineWidth: 2,
                    lineColor: null
                }
            }
        },
        tooltip: {
            valueSuffix: '',
            backgroundColor: '#222c3c',
            style: {
                "color": "#d9dfe8"
            },
            borderWidth: 0,
            borderRadius: 8,
            useHTML: true,
            headerFormat: '<div style="text-align:center">{point.key}</div>',
            formatter: function() {
                var result = '<div class="line-tooltip">';
                result += '<div class="tooltip-header" style="text-align:center">' + this.key + '</div>';
                result += '<div class="tooltip-body">'
                /*if (this.point.index > 1) {
                    result += '<div class="yesterday"><span class="key">' + intl.get('changes_yesterday') + '</span><span class="value">' + (this.series.data[this.point.index - 1].y - this.series.data[this.point.index - 2].y) + "</span></div>";
                } else {
                    result += '<div class="yesterday"><span class="key">' + intl.get('changes_yesterday') + '</span><span class="value">' + 0 + "</span></div>";
                }*/

                let changed = this.series.data[this.point.index].y - this.series.data[this.point.index - 1].y;

                if (this.point.index > 0) {
                    result += '<div class="today"><span class="key">' + (changed >= 0 ? ' + ' : ' - ') + '</span><span class="value">' + Math.abs(changed) + "</span></div>";
                } else {
                    result += '<div class="today"><span class="key"> + </span><span class="value">' + 0 + "</span></div>";
                }

                result += '</div>';
                return result;
            }
        },
        legend: {
            enabled: false
        },
        xAxis: {
            categories: catagories,
            labels: {
                style: {
                    color: '#adafb2',
                    fontSize: 12
                },
                formatter: function() {
                    var result = this.value.substr(5);
                    return result;
                }
            }
        },
        series
    };

    return (
        <ReactHighcharts config={config} />
    );
}

Line.propTypes = {
    statistic: PropTypes.any
};

export default Line;
