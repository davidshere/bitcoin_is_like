// Welcome to bil.js, bitcoinIsLike.js
//
// This is the code that runs the front end of bitcoin is like,
// an app that lets bitcoin lovers learn who out there shares their 
// agony or their extacy, whether you're up 1000% or down 90%. 

//////////////////////////////////////////////////////////////////////

// This section loads bitcoin data from the server into a variable 
// when the page loads. We'll send this data over to highcharts.js to 
// pleasingly display.

function fetchBTC(){
    $.getJSON('/_btc_history', function(btc){
        btc_series = btc;
        btc_dates = Object.keys(btc_series);
        btc_values = new Array;
        for(i in btc_dates){
            btc_values.push(btc_series[btc_dates[i]]);
        }
        btc_data = {'dates': btc_dates, 'values': btc_values};
        visualize_btc(btc_data); // I.e., you want to perform whatever operations you need the data for INSIDE the callback

    }
)}
$(document).ready(fetchBTC());

// Function to fetch a match based on input. 
// Once it fetches the match it should update the chart



function fetchMatch(){

    startDate = $("#start-date").val();
    endDate = $("#end-date").val();

    if (startDate===""){
        visualize_btc(btc_data);
    } else {
        $.post('/_fetch_match_series', {
            startDate: startDate,
            endDate: endDate 
        }).done(function(result){
            // separate out the pieces of the result object
            var company_name = result['company_name'];
            var matchSeries = result['series'];

            var matchDates = Object.keys(matchSeries);
            var matchSeriesStart = firstDateFromArray(matchDates);

            matchValues = reindexMatchSeries(matchSeries, matchDates);
            newBTCIndex = reindexBTCSeries(btc_series, matchSeriesStart);

            var indexedMatchSeries = {
                name: company_name,
                pointStart: matchSeriesStart,
                pointInterval: 24 * 3600 * 1000, // one day
                data: matchValues
            }

            var indexedBtcSeries = {
                name: 'bitcoin',
                pointStart: matchSeriesStart ,
                pointInterval: 24 * 3600 * 1000, // one day
                data: newBTCIndex
            }

            var chartOptions =  {
                    title: {
                        text: 'Bitcoin Is Like'
                    },
                    chart: {
                        type: 'line',
                        renderTo: 'chart'
                    },
                    xAxis: {
                        type: 'datetime',

                    },
                    yAxis: {
                        min: 0
                    },
                    series: [
                        indexedMatchSeries,
                        indexedBtcSeries
                    ]
                };
            chart = new Highcharts.Chart(chartOptions);
        });
    }
}

function visualize_btc(dates_and_values){
    var dates = dates_and_values['dates'];
    var values = dates_and_values['values'];
    
    var btc_start_time = firstDateFromArray(dates);

    var chartOptions =  {
                    title: {
                        text: 'Bitcoin Is Like'
                    },
                    chart: {
                        type: 'line',
                        renderTo: 'chart'
                    },
                    xAxis: {
                        type: 'datetime',

                    },
                    yAxis: {
                        min: 0
                    },
                    series: [{
                        name: 'bitcoin',
                        pointStart: btc_start_time,
                        pointInterval: 24 * 3600 * 1000, // one day
                        data: values
                    }]
                };
    chart = new Highcharts.Chart(chartOptions);
}

function firstDateFromArray(arrayObject){
    // Turns ["2014-01-01", "2014-01-02"...] into Date.UTC(2014, 01, 01)
    var first = arrayObject[0].split('-');
    var startTime = Date.UTC(first[0], first[1], first[2]);
    return startTime
}

function reindexMatchSeries(series, dates){
    var first_date = dates[0];
    var matchValues = new Array;
    var firstMatchValue = series[first_date];

    for(var i in dates){
        var indexed_value = series[dates[i]] / firstMatchValue;
        matchValues.push(indexed_value);
    }
    return matchValues;
}

function reindexBTCSeries(btc_series, first_date){ 
    // Here we're going to reindex the bitcoin data on the new series
    var newBTCSeries = new Array;
    for (var date in btc_series){ // populate array with dates following the minimum_start_date
        var dateArray = date.split('-');
        var utc_date = Date.UTC(dateArray[0], dateArray[1], dateArray[2]);
        if (first_date <= utc_date){ // we only want btc values past the minimum start date
            newBTCSeries.push(btc_series[date]);
        }
    }
    var firstBTCValue = newBTCSeries[0];
    var newBTCIndex = new Array;
    for(var i in newBTCSeries){
        newBTCIndex.push(newBTCSeries[i] / firstBTCValue)
    }
    return newBTCIndex
}


