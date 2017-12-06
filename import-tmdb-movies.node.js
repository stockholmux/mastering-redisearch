const 
  argv                = require('yargs')                                // `yargs` is a command line argument parser
                        .demand('connection')                           // pass in the node_redis connection object path with '--connection'
                        .demand('data')                                 // pass in the CSV file path with '--data'
                        .argv,                                          // return it back as a plain object
  async               = require('async'),                               // `async` provides some sugar for async execution
  parse               = require('csv-parse'),                           // `csv-parse` is a streaming parsing library
  fs                  = require('fs'),                                  // `fs` to access the file system
  redis               = require('redis'),                               // node_redis module
  rediSearchBindings  = require('redis-redisearch'),                    // supplies node_redis with the extended RediSearch commands
  rediSearch          = require('redisearchclient'),                    // RediSearch Abstraction library
  
  fields              = require('./fields.node.js'),                    // centralized schema for our specific data 
  connection          = require(argv.connection),                       // load and parse the JSON file at `argv.connection`
  
  client              = redis.createClient(connection),                 // establish the connection to redis with supplied connection
  parser              = parse({                                         // setup the parser with our options...
    delimiter: ',',                                                     // commas separate values
    columns : true,                                                     // first row is column names
    auto_parse : true                                                   // convert columns into the correct type
  }),
  ignoreFields        = [                                               // fields to ignore when importing
    'genres',
    'production_companies',
    'keywords',
    'production_countries',
    'spoken_languages'
  ],
  movieSearch         = rediSearch(client,'movies'),                    // setup the rediSearch client to the index key 'castCrew'                    
  q                   = async.queue(function(movie,done) {              // a queue that waits for items to be pushed in
    const movieId = movie.id;                                           // the ID of the movie, this will be our docId
    delete movie.id;                                                    // since it's our docId we don't need it in the value itself
  
    movieSearch.add(movieId,movie,function(err,resp) {                  // add the document to RediSearch
      if (err) { throw err; }                                           // handle errors 
      processed += 1;                                                   // increase the count of the movies parsed from the CSV to determine when we're done
      done(err);                                                        // return the queue for further processing
    });
  },2);                                                                 // this queue has a parallel value of 2

let
  parsed              = false,                                          // once CSV parsing is complete, we'll flip this
  processed           = 0,                                              // this is the count of items processed into RediSearch
  total               = 0;                                              // total items out of the CSV.

rediSearchBindings(redis);                                              // Apply the extend RediSearch commands to node_redis  

function csvRead(record) {                                              // Called every time the streaming CSV parser has data to read
  let movie = {};                                                       // Each row is a movie, we'll build up the plain object here

  Object.keys(record).forEach(function(aField) {                          // Grab the keys from the CSV record, then iterate over each field
    let thisCell;                                                       // home for cell data

    try {                                                               // This CSV may have embedded JSON in it...
      thisCell = JSON.parse(record[aField]);                              // so we need to try it to parse the JSON...
    } catch(e) {
      thisCell = record[aField];                                          // if it fails we assume it's not JSON and use it as-is
    } 
    if (aField === 'release_date') {                                      // if the field is the release date, then we need to do some further parsing
      thisCell = new Date(thisCell).getTime();                            // convert the YYYY-MM-DD into JS timestamp
    }
    if ((ignoreFields.indexOf(aField) === -1) && (thisCell)) {            // skip over any fields that are in the ignore array
      movie[aField] = thisCell;                                           // otherwise assign the cell to the correct field
    }
  });
  total += 1;                                                             // increase the total number of movie entries for determining the end of ingestion
  q.push(movie);                                                          // push the movie into the pipeline
}

q.drain = function() {                                                    // called when there are no more items to be processed
  if ((parsed) && (total === processed)) {                                // possible but unlikely that RediSearch is faster than CSV parsing, that we're done and everyhing is processed
    console.log('all done',total,'movies added.');
    client.quit();                                                        // release the client so node.js exits the process
  }
};



parser.on('readable', function(){                                         // when the stream gets a readable chunk
  while(record = parser.read()){ csvRead(record); }                       // handled CSV errors (should be none)
});
parser.on('error', function(err){                                         // when the last readable chunk has been procssed
  console.log('csv parse err',err.message);
});
parser.on('finish', function(){
  console.log('CSV Done.');
  parsed = true;                                                          // flip this so we know when we're done with this phase
});

console.log('Started ingesting of movies.')
movieSearch.createIndex(                                                  // create a new RediSearch index
  fields.movies(movieSearch),                                             // bring in the movie fields from our centralized fields module
  function(err) {
    if (err) { throw err; }                                               // handle the errors
    fs.createReadStream(argv.data).pipe(parser);                          // start reading the data from the file passed in through arguments and pipe that stream to the CSV parser
  }
);