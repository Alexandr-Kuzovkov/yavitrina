#!/usr/bin/env node

const leboncoin = require('leboncoin-api');
if (process.argv.length < 4){
    console.log('Usage: ' + process.argv[1] + ' <query> <page>');
    process.exit();
}
var query = process.argv[2];
var page = parseInt(process.argv[3], 10);
var search = new leboncoin.Search()
    .setPage(page)
    .setQuery(query)
    .setFilter(leboncoin.FILTERS.PROFESSIONNELS);

search.run().then(function (data) {
    result = {};
    result['page'] = data.page;
    result['nbResult'] = data.nbResult;
    result['results'] = (data.results);
    result['len'] = data.results.length;
    //console.log(data.page); // the current page
    //console.log(data.nbResult); // the number of results for this search
    //console.log(data.results); // the array of results
    console.log(JSON.stringify(result));

}, function (err) {
    console.error(err);
});
