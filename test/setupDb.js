// Copyright IBM Corp. 2017,2019. All Rights Reserved.
// Node module: loopback-connector-cassandra
// This file is licensed under the MIT License.
// License text available at https://opensource.org/licenses/MIT

var cassandra = require('cassandra-driver');

var keyspaceName = 'test';

function createKeySpaceForTest(cb) {
  var client = new cassandra.Client({ contactPoints: ['localhost'] });
  var sql = 'CREATE KEYSPACE IF NOT EXISTS "' + keyspaceName +
    '" WITH REPLICATION = { \'class\' : \'SimpleStrategy\',' +
    ' \'replication_factor\' : \'3\' }';
  client.execute(sql, function(err) {
    process.exit(0);
  });
}

createKeySpaceForTest();
