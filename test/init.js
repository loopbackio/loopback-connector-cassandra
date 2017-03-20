var DataSource = require('loopback-datasource-juggler').DataSource;

var config = require('rc')('loopback', {test: {cassandra: {
  host: 'localhost',
  keyspace: 'test'
}}}).test.cassandra;

global.getDataSource = global.getSchema = function() {
  var db = new DataSource(require('../'), config);
  return db;
};

global.connectorCapabilities = {
  ilike: false,
  nilike: false,
  nestedProperty: false,
};

global.sinon = require('sinon');
