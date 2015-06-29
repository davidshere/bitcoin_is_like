// Welcome to bil.js, bitcoinIsLike.js
//
// This is the code that runs the front end of bitcoin is like,
// an app that lets bitcoin lovers learn who out there shares their 
// agony or their extacy, whether you're up 1000% or down 90%. 

//////////////////////////////////////////////////////////////////////

// This section loads bitcoin data from the server into a variable 
// when the page loads. We'll send this data over to highcharts.js to 
// pleasingly display.

function fetch_btc(){
    $.getJSON('/_btc_history', function(btc){
        btc_series = btc;
        btc_dates = Object.keys(btc_series);
        btc_values = new Array;
        for(i in btc_dates){
            btc_values.push(btc_series[btc_dates[i]]);
        }
        var btc_data = {'dates': btc_dates, 'values': btc_values};
        visualize_btc(btc_data); // I.e., you want to perform whatever operations you need the data for INSIDE the callback

    }
)}
$(document).ready(fetch_btc);

// Function to fetch a match based on input. 
// Once it fetches the match it should update the chart



function fetchMatch(){

    startDate = $("#start-date").val();
    endDate = $("#end-date").val();

    if(startDate===""){ // if there's no start_date
        fetch_btc()
    } else {
        $.post('/_fetch_match_series', {
            startDate: startDate,
            endDate: endDate 
        }).done(function(result){
            // separate out the pieces of the result object
            var company_name = result['company_name'];
            var matchSeries = result['series'];

            // instantiage 

            var dates = Object.keys(matchSeries);
            var matchValues = new Array;


            // Now we've got to process the series to properly index
            var first_match_date = dates[0]
            var first_date_array = first_match_date.split('-');
            match_series_start_time = Date.UTC(first_date_array[0], 
                                               first_date_array[1], 
                                               first_date_array[2]);
            var firstMatchValue = matchSeries[first_match_date]

            for(i in dates){
                var indexed_value = matchSeries[dates[i]] / firstMatchValue;
                matchValues.push(indexed_value);
            }

            // Here we're going to reindex the bitcoin data on the new series
            newBTCSeries = new Array;

            for (date in btc_series){ // populate array with dates following the minimum_start_date
                dateArray = date.split('-');
                utc_date = Date.UTC(dateArray[0], dateArray[1], dateArray[2]);
                if (match_series_start_time <= utc_date){ // we only want btc values past the minimum start date
                    newBTCSeries.push(btc_series[date]);
                }
            }
            var firstBTCValue = newBTCSeries[0];
            var newBTCIndex = new Array;
            for(i in newBTCSeries){
                newBTCIndex.push(newBTCSeries[i] / firstBTCValue)
            }

            var matchSeries = {
                name: company_name,
                pointStart: match_series_start_time,
                pointInterval: 24 * 3600 * 1000, // one day
                data: matchValues
            }

            var indexedBtcSeries = {
                name: 'bitcoin',
                pointStart: match_series_start_time ,
                pointInterval: 24 * 3600 * 1000, // one day
                data: newBTCIndex
            }

            var chartOptions =  {
                    title: {
                        text: 'bitcoin is like'
                    },
                    chart: {
                        type: 'line',
                        renderTo: 'chart'
                    },
                    xAxis: {
                        type: 'datetime',

                    },
                    series: [
                        matchSeries,
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
    
    // Need to create a date object to start my chart
    var first = dates[0].split('-');
    var btc_start_time = Date.UTC(first[0], first[1], first[2]);

    $(function () {
        var chartOptions =  {
                        title: {
                            text: 'bitcoin is like'
                        },
                        chart: {
                            type: 'line',
                            renderTo: 'chart'
                        },
                        xAxis: {
                            type: 'datetime',

                        },
                        series: [{
                            name: 'bitcoin',
                            pointStart: btc_start_time,
                            pointInterval: 24 * 3600 * 1000, // one day
                            data: btc_values
                        }]
                    };
        chart = new Highcharts.Chart(chartOptions);
    });
}