// Welcome to bil.js.
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

function fetchLastMatchDate(){
    // need this to make sure we don't try to query for a date that doesn't exist
    $.getJSON('/_last_match', function(result){
        last_match_date = moment(result['date']);
    })
}

function onPageLoad(){
    fetchBTC();
    fetchLastMatchDate();
}
$(document).ready(onPageLoad());

// Function to fetch a match based on input. 
// Once it fetches the match it should update the chart
function emptyInvalidInput(){
    $("#invalid-input").empty();
}

function dateStringValidator(startDate){
    if (!moment(startDate).isValid()) { 
        var invalidDateFormat = "<p>I'm sorry, you've entered an invalid date. The proper format is YYYY-MM-DD<p>";
        $('#invalid-input').html(invalidDateFormat).css('color', 'red');
        return false;
    } else if (moment(startDate) > last_match_date) {
        var dateTooLate = "<p>I'm sorry, we don't have a match for that date. Please try another.<p>";
        $('#invalid-input').html(dateTooLate).css('color', 'red');
        return false;
    } else {
        return true;
    }
}

function fetchMatch(){
    startDate = $("#start-date").val();
    if (startDate===""){
        emptyInvalidInput();
        visualize_btc(btc_data);
        return 0;
    } 

    if (dateStringValidator(startDate)) {
        $.post('/_fetch_match_series', {
            startDate: startDate
        }).done(function(matchDataFromServer){
            vizualizeMatch(matchDataFromServer);
            emptyInvalidInput();
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

function vizualizeMatch(result){
    // separate out the pieces of the result object
    var matchSeries = result['series'];

    var matchDates = Object.keys(matchSeries);
    var matchSeriesStart = firstDateFromArray(matchDates);

    matchValues = processMatchSeries(matchSeries, matchDates);
    newBTCIndex = processBTCSeries(btc_series, matchSeriesStart);
    
    var company_name = result['company_name'];

    var indexedMatchSeries = {
        name: company_name,
        pointStart: matchSeriesStart,
        pointInterval: 24 * 3600 * 1000, // one day
        data: matchValues
    }

    var indexedBtcSeries = {
        name: 'bitcoin',
        pointStart: matchSeriesStart,
        yAxis: 1,
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
            yAxis: [{
                min: 0,
            },
            {
                min: 0,
                opposite: true
            }],
            series: [
                indexedMatchSeries,
                indexedBtcSeries
            ]
        };
    chart = new Highcharts.Chart(chartOptions);
}

function firstDateFromArray(arrayObject){
    // Turns ["2014-01-01", "2014-01-02"...] into Date.UTC(2014, 01, 01)
    var first = arrayObject[0].split('-');
    var startTime = Date.UTC(first[0], first[1], first[2]);
    return startTime
}

function processMatchSeries(series, dates){
    var first_date = dates[0];
    var matchValues = new Array;
    var firstMatchValue = series[first_date];

    for(var i in dates){
        var indexed_value = series[dates[i]] / firstMatchValue;
        matchValues.push(series[dates[i]]);
    }
    return matchValues;
}

function processBTCSeries(btc_series, first_date){ 
    // Here we're going to trim the bitcoin data to start on the start date
    var newBTCSeries = new Array;
    for (var date in btc_series){ 
        var dateArray = date.split('-');
        var utc_date = Date.UTC(dateArray[0], dateArray[1], dateArray[2]);
        if (first_date <= utc_date){ // we only want btc values past the minimum start date
            newBTCSeries.push(btc_series[date]);
        }
    }
    return newBTCSeries
}




