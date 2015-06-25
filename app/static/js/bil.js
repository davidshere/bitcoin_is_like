// Welcome to bil.js, bitcoinIsLike.js
//
// This is the code that runs the front end of bitcoin is like,
// an app that lets bitcoin lovers learn who out there shares their 
// agony or their extacy, whether you're up 1000% or down 90%. 

//////////////////////////////////////////////////////////////////////

// This section loads bitcoin data from the server into a variable 
// when the page loads. We'll send this data over to highcharts.js to 
// pleasingly display.




/*
var get_btc_dates = function(btc_obj) {
}
$(document).ready(get_btc_dates);

var get_btc_values = function(){
    values = new Array;

    return values;
}
$(document).ready(get_btc_values);



var btc_series = (function (){
    $.getJSON('/_btc_history', function(btc){
        return btc;
    }
)}());
keys = Object.keys(btc_series);
for (key in keys) {
    values.push(btc_series[key]);
}
*/

function fetch_btc(){
    $.getJSON('/_btc_history', function(btc){
        var btc_series = btc;
        var dates = Object.keys(btc_series);
        var values = new Array;
        for(i in dates){
            //console.log(key);
            values.push(btc_series[dates[i]]);
        }
        console.log(values); // for my own sanity
        var btc_data = {'dates': dates, 'values': values};
        visualize(btc_data); // I.e., you want to perform whatever operations you need the data for INSIDE the callback

    }
)}
$(document).ready(fetch_btc);



/*
function fetch_btc(){
    $.getJSON('/_btc_history', function(btc){
        var btc_series = btc;
        var keys = Object.keys(btc_series);
        var btc_data = [btc_series, keys];
        return btc_data

    }
)}
$(document).ready(fetch_btc);

k = Object.keys(btc_series);
console.log(k);
*/
function visualize(dates_and_values){
    var dates = dates_and_values['dates'];
    var values = dates_and_values['values'];
    
    // Need to create a date object to start my chart
    var first = dates[0].split('-');
    var start_time = Date.UTC(first[0], first[1], first[2]);
    console.log(start_time);

    $(function () {
        $('#chart').highcharts({
            title: {
                text: 'bitcoin is like'
            },
            chart: {
                type: 'line'
            },
            xAxis: {
                type: 'datetime',

            },

            series: [{
                pointStart: start_time,
                pointInterval: 24 * 3600 * 1000, // one day
                data: values
            }]
        });
    });
}
// CHART CITY!
/*
$(function () {
    $('#chart').highcharts({
        title: {
            text: 'Bitcoin Is Like',
            x: -20 //center
        },
        subtitle: {
            text: 'Powered by BitcoinAverage.com and Quandl',
            x: -20
        },
        xAxis: {
            categories: ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun',
                'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
        },
        yAxis: {
            title: {
                text: 'Temperature (°C)'
            },
            plotLines: [{
                value: 0,
                width: 1,
                color: '#808080'
            }]
        },
        tooltip: {
            valueSuffix: '°C'
        },
        legend: {
            layout: 'vertical',
            align: 'right',
            verticalAlign: 'middle',
            borderWidth: 0
        },
        series: [{
            name: 'Tokyo',
            data: [7.0, 6.9, 9.5, 14.5, 18.2, 21.5, 25.2, 26.5, 23.3, 18.3, 13.9, 9.6]
        }, {
            name: 'New York',
            data: [-0.2, 0.8, 5.7, 11.3, 17.0, 22.0, 24.8, 24.1, 20.1, 14.1, 8.6, 2.5]
        }, {
            name: 'Berlin',
            data: [-0.9, 0.6, 3.5, 8.4, 13.5, 17.0, 18.6, 17.9, 14.3, 9.0, 3.9, 1.0]
        }, {
            name: 'London',
            data: [3.9, 4.2, 5.7, 8.5, 11.9, 15.2, 17.0, 16.6, 14.2, 10.3, 6.6, 4.8]
        }]
    });
});
*/