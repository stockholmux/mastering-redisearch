const 
  argv    = require('yargs')
          .demand('searchtype')
          .choices('searchtype',['movies','castCrew'])
          .demand('connection')
          .demand('search')
          .argv,
  redis   = require('redis'),
  connection
          = require(argv.connection),
  rediSearchBindings
          = require('..//node_redis-redisearch'),
  rediSearch  
          = require('redisearchclient'),
  client  = redis.createClient(connection),
  data    = rediSearch(client,argv.searchtype);

rediSearchBindings(redis);
function showResults(err,results) {
  if (err) { throw err; }
  console.log(JSON.stringify(results,null,2));
  client.quit();
}    

searchOpts = {};

if (argv.offset) { searchOpts.offset = Number(argv.offset); }
if (argv.resultsize) { searchOpts.numberOfResults = Number(argv.resultsize); }

data.search(
  argv.search,
  searchOpts,
  showResults
);