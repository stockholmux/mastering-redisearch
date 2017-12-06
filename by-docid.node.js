const 
  argv    = require('yargs')
          .demand('searchtype')
          .choices('searchtype',['movies','castCrew'])
          .demand('connection')
          .demand('docid')
          .argv,
  connection
          = require(argv.connection),
  redis   = require('redis'),
  rediSearch  
          = require('redisearchclient'),
  data
          = rediSearch(redis,argv.searchtype,{ clientOptions : connection });

const showResults = function(err,results){
  if (err) { throw err };
  console.log(JSON.stringify(results,null,2));
  data.client.quit();
};
data.getDoc(argv.docid,showResults);
