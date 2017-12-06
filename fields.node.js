module.exports = {
  movies    : function(search) {
    return [
      search.fieldDefinition.numeric('budget',true),
      //homepage is just stored, not indexed
      search.fieldDefinition.text('original_language',true,{ noStem : true }),
      search.fieldDefinition.text('original_title',true,{ weight : 4.0 }),
      search.fieldDefinition.text('overview',false),
      search.fieldDefinition.numeric('popularity'),
      search.fieldDefinition.numeric('release_date',true),
      search.fieldDefinition.numeric('revenue',true),
      search.fieldDefinition.numeric('runtime',true),
      search.fieldDefinition.text('status',true,{ noStem : true }),
      search.fieldDefinition.text('title',true,{ weight : 5.0 }),
      search.fieldDefinition.numeric('vote_average',true),
      search.fieldDefinition.numeric('vote_count',true)
    ];
  },
  castCrew  : function(search) {
    return [
      search.fieldDefinition.numeric('movie_id',false),
      search.fieldDefinition.text('title',true, { noStem : true }),
      search.fieldDefinition.numeric('cast',true),
      search.fieldDefinition.numeric('crew',true),
      search.fieldDefinition.text('name', true, { noStem : true }),
  
      //cast only
      search.fieldDefinition.text('character', true, { noStem : true }),
      search.fieldDefinition.numeric('cast_id',false),
  
      //crew only
      search.fieldDefinition.text('department',true),
      search.fieldDefinition.text('job',true)
    ];
  }
};