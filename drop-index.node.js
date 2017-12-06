const
  argv                = require('yargs')
                        .demand('connection')
                        .demand('searchtype')
                        .choices('searchtype',['movies','castCrew'])
                        .argv,
  redis               = require('redis'),
  connection          = require(argv.connection),
  rediSearchBindings  = require('redis-redisearch'),
  rediSearch          = require('redisearchclient'),
  client              = redis.createClient(connection),
  data                = rediSearch(client,argv.searchtype);
            
rediSearchBindings(redis);

data.dropIndex(function(err) {
  if (err) { throw err; }
  console.log('Index Dropped for',argv.searchtype);
  client.quit();
});