// Copyright IBM Corp. 2015,2017. All Rights Reserved.
// Node module: loopback-connector-cassandra
// This file is licensed under the MIT License.
// License text available at https://opensource.org/licenses/MIT

var DataSource = require('loopback-datasource-juggler').DataSource;

var config = require('rc')('loopback', {test: {cassandra: {
  host: process.env.CASSANDRA_HOST || 'localhost',
  port: process.env.CASSANDRA_PORT || 9042,
  keyspace: process.env.CASSANDRA_KEYSPACE || 'test'
}}}).test.cassandra;

global.getDataSource = global.getSchema = function() {
  var db = new DataSource(require('../'), config);
  return db;
};

global.connectorCapabilities = {
  ilike: false,
  nilike: false,
  nestedProperty: false,
  adhocSort: false,
  supportInq: false,
  reportDeletedCount: false,
  deleteWithOtherThanId: false,
  updateWithOtherThanId: false,
  supportOrOperator: false,
  refuseDuplicateInsert: false,
  supportForceId: false,
  updateWithoutId: false,
  ignoreUndefinedConditionValue: false,
  supportStrictDelete: false,
  supportPagination: false,
  supportUpdateWithoutId: false,
  supportInclude: false,
  supportGeoPoint: false,
  nullDataValueExists: false,
  supportTwoOrMoreInq: false,
};

global.sinon = require('sinon');
