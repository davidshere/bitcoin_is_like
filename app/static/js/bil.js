`// Welcome to bil.js.
//
// This is the code that runs the front end of bitcoin is like,
// an app that lets bitcoin lovers learn who out there shares their 
// agony or their extacy, whether you're up 1000% or down 90%. 

//////////////////////////////////////////////////////////////////////

// Section: On page load
// Action:
//  Fetch Bitcoin data from server
//  Fetch match start and end dates from server

function fetchBTC(){
    $.getJSON('/_btc_history', function(btc){
        btc_series = btc;
        var btc_dates = Object.keys(btc_series);
        var btc_values = new Array;
        for(i in btc_dates){
            btc_values.push(btc_series[btc_dates[i]]);
        }
        btc_data = {'dates': btc_dates, 'values': btc_values};
        visualize_btc(btc_data); // I.e., you want to perform whatever operations you need the data for INSIDE the callback

    }
)}

function fetchMatchDateRange(){
    // need this to make sure we don't try to query for a date that doesn't exist
    $.getJSON('/_match_range', function(result){
        earliest_match_date = moment(result['earliest'])
        latest_match_date = moment(result['latest']);
    })
}

function onPageLoad(){
    fetchBTC();
    fetchMatchDateRange();
}
$(document).ready(onPageLoad());

//
// Section: Fetchin the match data from the server
//

function emptyInvalidInput(){
    $("#invalid-input").empty();
}

function dateStringValidator(startDate){
    var momentStart = moment(startDate);
    if (!momentStart.isValid()) { 
        var invalidDateFormat = "<p>I'm sorry, you've entered an invalid date. The proper format is YYYY-MM-DD<p>";
        $('#invalid-input').html(invalidDateFormat).css('color', 'red');
        return false;
    } else if (momentStart < latest_match_date || momentStart > earliest_match_date) {
        console.log(momentStart > latest_match_date);
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

//
//  Section: visualizing the data
//

function visualize_btc(dates_and_values){
    var dates = dates_and_values['dates'];
    var values = dates_and_values['values'];
    var btc_start_time = dateFromString(dates[0]);

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
    var matchData = result['series'];
    var company_name = result['company_name'];

    var btcSeries = matchData['btc'];
    var matchDates = matchData['date'];
    var matchDateStart = matchDates[0]
    var matchSeries = matchData['series'];
    var lastDate = matchDates.pop();
    matchValues = processMatchSeries(matchSeries, matchDates);
    newBTCIndex = processBTCSeries(btcSeries, matchDates);

    var indexedMatchSeries = {
        name: company_name,
        pointStart: matchDateStart,
        pointInterval: 24 * 3600 * 1000, // one day
        data: matchValues
    }

    var indexedBtcSeries = {
        name: 'bitcoin',
        pointStart: matchDateStart,
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
                type: 'datetime'
            },
            yAxis: {
                min:0
            },
            series: [
                indexedMatchSeries,
                indexedBtcSeries
            ]
        };
    chart = new Highcharts.Chart(chartOptions);
}


// Section: Utility functions
function dateFromString(dateString){
    var dateArray = dateString.split("-");
    var date = Date.UTC(dateArray[0], dateArray[1] - 1, dateArray[2]); // month -1 because dates go from 0-11, not 1-12
    return date
}


//
//  Section: process data (move this to the server!
//
function processMatchSeries(series, dates){
    // TODO: We should do this on the server, or better yet the databaes!
    var matchValues = new Array;
    var firstMatchValue = series[0];
    for(var i in dates){
        var indexedValue = series[i] / firstMatchValue;
        var date = dateFromString(dates[i]);
        var dataPoint = [date, indexedValue]
        matchValues.push(dataPoint);
    }
    return matchValues;
}

function processBTCSeries(btc_series, dates){ 
    // Here we're going to trim the bitcoin data to start on the start date
    // TODO: We should do this on the server, or better yet the database!
    //       If for some reason we have to do it on the client, let's start
    //       immediately after the AJAX call, no point waiting around
    var firstDate = dateFromString(dates[0]);
    var firstBTCVal = btc_series[0];
    var newBTCSeries = new Array;
    for (var i in btc_series){
        if (dates[i]){ // we only want to run this if there's a date there
            var utc_date = dateFromString(dates[i]);
            if (firstDate <= utc_date){ // we only want btc values past the minimum start date
                var indexedValue = btc_series[i] / firstBTCVal
                var dataPoint = [utc_date, indexedValue]
                newBTCSeries.push(dataPoint);
            }
        } else { // if there's no dates[i]
            console.log('not found ', i, dates[i - 1]);
        }         
    }
    return newBTCSeries
}




