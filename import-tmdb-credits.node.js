const 
  argv                = require('yargs')                                // `yargs` is a command line argument parser
                        .demand('connection')                           // pass in the node_redis connection object path with '--connection'
                        .demand('data')                                 // pass in the CSV file path with '--data'
                        .argv,                                          // return it back as a plain object
  async               = require('async'),                               // `async` provides some sugar for async execution
  _                   = require('lodash'),                              // `lodash` provides sugar for transformations
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
  castCrew            = rediSearch(client,'castCrew'),                  // setup the rediSearch client to the index key 'castCrew'        
  q                   = async.queue(function(batchToExec,cb) {          // a queue that waits for items to be pushed in
    batchToExec.exec(function(err,results) {                            // `batchToExec` a batch that is passed through arguments, now we execute
      if (err) { cb(err); } else {                                      // handle errors
        processed += results.length;                                    // increase the count so we can keep track of the number of processed in case of odd race condition
        cb(err,results);                                                // move on to the next batch
      }
    });
  },2);                                                                 // this queue has a parallel value of 2

let   
  parsed    = false,                                                    // once CSV parsing is complete, we'll flip this
  processed = 0,                                                        // this is the count of items processed into RediSearch
  total     = 0;                                                        // total items out of the CSV.

rediSearchBindings(redis);                                              // Apply the extend RediSearch commands to node_redis

function csvRead(){                                                     // Called every time the streaming CSV parser has data to read
  let
    movieBatch = castCrew.batch(),                                      // start a pipeline
    record;                                                             // record = a row from the CSV

  while(record = parser.read()){                                        // while there is data to read, loop
    let 
      castCrewMovieInfo = {                                             // this will be the basis of multiple RediSearch docs
        movie_id  : record.movie_id,
        title     : record.title
      };

    _(record).keys().forEach(function(aKey) {                           // grab the keys (aka columns) from the row and iterate
      let
        parsed;                                                         // we'll store the parsed value of the cell in this variable

      try {                                                             // This CSV may have embedded JSON in it...
        parsed = JSON.parse(record[aKey]);                              // so we need to try it to parse the JSON...
      } catch(e) {
        parsed = record[aKey];                                          // if it fails we assume it's not JSON and use it as-is
      } 
      if (aKey === 'cast') {                                            // if we are in the cast column.
        record[aKey] = parsed.map(function(aCastObj) {                  // this cell will have an array of objects
          return Object.assign({}, castCrewMovieInfo, {                 // we'll create a new object based on an empty plain object then add in all common element from the movie 
            character : aCastObj.character,                             // and we cherry-pick the relevant cast credit fields
            cast_id   : aCastObj.id,
            name      : aCastObj.name,
            credit_id : aCastObj.credit_id,                             // this is a unique value that will end up being our docId
            cast      : 1                                               // setting the cast flag to 1
          });
        });
      } else if (aKey === 'crew') {                                     // if we are in the crew column.
        record[aKey] = parsed.map(function(aCrewObj) {                  // this cell will have an array of objects
          return Object.assign({}, castCrewMovieInfo, {                 // and we cherry-pick the relevant crew credit fields
            department  : aCrewObj.department,
            job         : aCrewObj.job,
            name        : aCrewObj.name,
            credit_id   : aCrewObj.credit_id,                           // this is a unique value that will end up being our docId
            crew        : 1                                             // setting the cast flag to 1
          });
        });
      }
    });                                                                 // we end up with two columns that contain multiple cast/crew that will be docs in RediSearch
    _.union(record.crew, record.cast)                                   // we take these two columns and put all the elements together                                
      .forEach(function(aCastOrCrewObj) {                               // then we iterate
        let creditId = aCastOrCrewObj.credit_id;                        // grab the `creditId`
        delete  aCastOrCrewObj.credit_id;                               // we don't need it in the document though, since it will be the docId
        total += 1;                                                     // increase the total number of cast/crew entries for determining the end of ingestion
        movieBatch.rediSearch.add(creditId, aCastOrCrewObj);            // add the document to the pipeline
      });
    q.push(movieBatch);                                                 // push the pipeline to the queue for processing
  }
}

q.drain = function() {                                                  // called when there are no more items to be processed
  if ((parsed) && (total === processed)) {                              // possible but unlikely that RediSearch is faster than CSV parsing, that we're done and everyhing is processed
    console.log('all done',total,'credits added.');
    client.quit();                                                      // release the client so node.js exits the process
  }
}

parser.on('readable', csvRead);                                         // when the stream gets a readable chunk
parser.on('error', function(err){                                       // handled CSV errors (should be none)
  console.log('csv parse err',err.message);
});
parser.on('finish', function(){                                         // when the last readable chunk has been procssed
  console.log('CSV Done.');
  parsed = true;                                                        // flip this so we know when we're done with this phase
});

console.log('Started ingesting of credits.');
castCrew.createIndex(                                                   // create a new RediSearch index
  fields.castCrew(castCrew),                                            // bring in the cast/crew fields from our centralized fields module
  function(err) {
    if (err) { throw err; }                                             // handle the errors
    fs.createReadStream(argv.data).pipe(parser);                        // start reading the data from the file passed in through arguments and pipe that stream to the CSV parser
  }
);