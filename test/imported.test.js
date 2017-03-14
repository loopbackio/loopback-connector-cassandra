describe('cassandra custom and imported features', function () {

  before(function () {
    require('./init.js');
  });

  require('./cass.custom.test.js');
  require('loopback-datasource-juggler/test/common.batch.js');
  require('loopback-datasource-juggler/test/include.test.js');

});
