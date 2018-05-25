// Copyright IBM Corp. 2017. All Rights Reserved.
// Node module: loopback-connector-cassandra
// This file is licensed under the MIT License.
// License text available at https://opensource.org/licenses/MIT

// This test written in mocha+should.js
'use strict';

/* global getSchema:false */

var DataSource = require('loopback-datasource-juggler').DataSource;
var cassandra = require('cassandra-driver');
var cassConnector = require('../lib/cassandra');
var should = require('should');

var db, CASS, CASS_SORTABLE, CASS_TUPLE_TIME;
var ID, ROW;
var cassTestString = 'cassandra test string data';
var cassTestNum = 3;

describe('cassandra custom tests', function() {
  before(function(done) {
    db = getSchema();
    CASS = db.define('CASS', {
      str: String,
      num: Number,
      });
    CASS_SORTABLE = db.define('CASS_SORTABLE', {
      patBool: {type: Boolean, id: 2},
      str: String,
      patStr: {type: String, id: true},
      num: Number,
      patNum: {type: Number, id: 1},
      yearMonth: {type: String, index: true},
      }, {
      cassandra: {
        clusteringKeys: ['str', 'num DESC'],
        },
      });
    CASS_TUPLE_TIME = db.define('CASS_TUPLE_TIME', {
      tuple: {type: 'Tuple'},
      str: String,
      num: Number,
      time: {type: 'TimeUuid', id: true},
      });
    db.automigrate(['CASS', 'CASS_SORTABLE', 'CASS_TUPLE_TIME'], done);
  });

  function verifyTheDefaultRows(err, m) {
    should.not.exists(err);
    should.exist(m && m.id);
    should.exist(m && m.str);
    should.exist(m && m.num);
    m.str.should.be.type('string');
    m.str.indexOf(cassTestString).should.be.aboveOrEqual(0);
    m.num.should.be.type('number');
    m.num.should.be.aboveOrEqual(cassTestNum);
  }

  function verifyExtraRows(err, m) {
    should.not.exists(err);
    should.exist(m && m.patBool);
    should.exist(m && m.patNum);
    should.exist(m && m.patStr);
    m.patBool.should.be.type('boolean');
    m.patBool.should.equal(true);
    m.patNum.should.be.type('number');
    m.patNum.should.equal(100);
    m.patStr.should.be.type('string');
    m.patStr.should.equal(cassTestString + '100');
 }

  describe('create keyspace if it does not exist', function () {
    it('create keysapce with no specified replication', function (done) {
      var config = require('rc')('loopback', {
        test: {
          cassandra: {
            host: process.env.CASSANDRA_HOST || 'localhost',
            port: process.env.CASSANDRA_PORT || 9042,
            keyspace: 'test0_' + Date.now(),
            createKeyspace: true,
          }
        }
      }).test.cassandra;

      var ds = new DataSource(require('../'), config);

      cassConnector.initialize(ds, function () {
        ds.connector.client.execute('SELECT replication FROM system_schema.keyspaces WHERE keyspace_name=\'' + config.keyspace + '\'', function (err, rows) {
          rows.rows[0].replication.class.should.eql('org.apache.cassandra.locator.SimpleStrategy');
          rows.rows[0].replication.replication_factor.should.eql('3');
          done();
        });
      });
    });

    it('create keysapce with specified replication', function (done) {
      var config = require('rc')('loopback', {
        test: {
          cassandra: {
            host: process.env.CASSANDRA_HOST || 'localhost',
            port: process.env.CASSANDRA_PORT || 9042,
            keyspace: 'test1_' + Date.now(),
            createKeyspace: true,
            replication: {
              class: 'NetworkTopologyStrategy', 
              dc1: 1,
            }
          }
        }
      }).test.cassandra;

      var ds = new DataSource(require('../'), config);

      cassConnector.initialize(ds, function () {
        ds.connector.client.execute('SELECT replication FROM system_schema.keyspaces WHERE keyspace_name=\'' + config.keyspace + '\'', function (err, rows) {
          rows.rows[0].replication.class.should.eql('org.apache.cassandra.locator.NetworkTopologyStrategy');
          rows.rows[0].replication.dc1.should.eql('1');
          done();
        });
      });
    });
  });

  // http://apidocs.strongloop.com/loopback/#persistedmodel-create
  it('create', function(done) {
    CASS.create({
      str: cassTestString,
      num: cassTestNum,
    }, function(err, m) {
      verifyTheDefaultRows(err, m);
      ROW = m;
      ID = m.id;
      done();
    });
  });

  // http://apidocs.strongloop.com/loopback/#persistedmodel-findbyid
  it('findOne', function(done) {
    CASS.findOne({where: {id: ID}}, function(err, m) {
      verifyTheDefaultRows(err, m);
      done();
    });
  });

  // http://apidocs.strongloop.com/loopback/#persistedmodel-findbyid
  it('findById', function(done) {
    CASS.findById(ID, function(err, m) {
      verifyTheDefaultRows(err, m);
      done();
    });
  });

  // http://apidocs.strongloop.com/loopback/#persistedmodel-prototype-updateattributes
  it('destroyAll with id', function(done) {
    CASS.destroyAll({id: ID},
    function(err, info) {
      should.not.exist(err);
      should.exist(info);
      should.not.exist(info.count);
      // however, info.count is not set. :-(
      // info.count.should.be.type('number');
      done();
    });
  });

  // http://apidocs.strongloop.com/loopback/#persistedmodel-create
  it('re-create', function(done) {
    CASS.create({
      str: cassTestString,
      num: cassTestNum,
    }, function(err, m) {
      verifyTheDefaultRows(err, m);
      ROW = m;
      ID = m.id;
      done();
    });
  });

  // http://apidocs.strongloop.com/loopback/#persistedmodel-prototype-updateattributes
  it('updateAttributes', function(done) {
    ROW.updateAttributes({
      id: ID, str: cassTestString + '2', num: cassTestNum + 1,
    }, function(err, instance) {
      should.not.exists(err);
      instance.id.should.be.equal(ID);
      instance.str.should.be.type('string');;
      instance.str.should.be.equal(cassTestString + '2');;
      instance.num.should.be.type('number');;
      instance.num.should.be.equal(cassTestNum + 1);;
      done();
    });
  });

  // http://apidocs.strongloop.com/loopback/#persistedmodel-destroyall
  it('destroyAll without id', function(done) {
    CASS.destroyAll({},
    function(err, info) {
      should.not.exist(err);
      should.not.exist(info.count);
      // however, info.count is not set. :-(
      // info.count.should.be.type('number');
      done();
    });
  });

  var ID_1;

  it('create sortable 1', function(done) {
    CASS_SORTABLE.create({
      str: cassTestString + '10',
      num: 10,
      patBool: true,
      patNum: 100,
      patStr: cassTestString + '100',
    }, function(err, m) {
      verifyTheDefaultRows(err, m);
      verifyExtraRows(err, m);
      m.id.should.not.have.property('id');
      m.id.should.have.properties(
        {patStr: 'cassandra test string data100',
        patNum: 100,
        patBool: true });
      ID_1 = m.id;
      done();
    });
  });

  it('create sortable 2', function(done) {
    CASS_SORTABLE.create({
      str: cassTestString + '20',
      num: 20,
      patBool: true,
      patNum: 100,
      patStr: cassTestString + '100',
    }, function(err, m) {
      verifyTheDefaultRows(err, m);
      verifyExtraRows(err, m);
      m.id.should.have.properties(ID_1);
      m.id.should.not.have.property('id');
      m.id.should.have.properties(
        {patStr: 'cassandra test string data100',
        patNum: 100,
        patBool: true });
      done();
    });
  });

  it('create sortable 3', function(done) {
    CASS_SORTABLE.create({
      str: cassTestString + '20',
      num: 30,
      patBool: true,
      patNum: 100,
      patStr: cassTestString + '100',
    }, function(err, m) {
      verifyTheDefaultRows(err, m);
      verifyExtraRows(err, m);
      m.id.should.have.properties(ID_1);
      m.id.should.not.have.property('id');
      m.id.should.have.properties(
        {patStr: 'cassandra test string data100',
        patNum: 100,
        patBool: true });
      done();
    });
  });

  it('find and order by str', function(done) {
    CASS_SORTABLE.find(
      {where: {id: ID_1},
      order: 'str'}, function(err, rows) {
        should.not.exist(err);
        rows.should.have.length(3); // str ASC
        rows[0].str.should.eql('cassandra test string data10');
        rows[1].str.should.eql('cassandra test string data20');
        rows[2].str.should.eql('cassandra test string data20');
        done();
      });
  });

  it('find and order by num', function(done) {
    CASS_SORTABLE.find(
      {where: {and: [{id: ID_1},{str: 'cassandra test string data20'}]},
      order: 'num'}, function(err, rows) {
        should.not.exist(err);
        rows.should.have.length(2); // num DESC
        rows[0].num.should.eql(30);
        rows[1].num.should.eql(20);
        done();
      });
  });

  it('create sortable 4', function(done) {
    CASS_SORTABLE.create({
      str: cassTestString + '50',
      num: 40,
      patBool: true,
      patNum: 100,
      patStr: cassTestString + '100',
      yearMonth: '2015-03',
    }, function(err, m) {
      verifyTheDefaultRows(err, m);
      verifyExtraRows(err, m);
      m.id.should.have.properties(ID_1);
      m.id.should.not.have.property('id');
      m.should.have.properties({yearMonth: '2015-03'});
      done();
    });
  });

  it('create sortable 5', function(done) {
    CASS_SORTABLE.create({
      str: cassTestString + '50',
      num: 50,
      patBool: true,
      patNum: 100,
      patStr: cassTestString + '100',
      yearMonth: '2015-04',
    }, function(err, m) {
      verifyTheDefaultRows(err, m);
      verifyExtraRows(err, m);
      m.id.should.have.properties(ID_1);
      m.id.should.not.have.property('id');
      m.should.have.properties({yearMonth: '2015-04'});
      done();
    });
  });

  it('create sortable 6', function(done) {
    CASS_SORTABLE.create({
      str: cassTestString + '50',
      num: 60,
      patBool: true,
      patNum: 100,
      patStr: cassTestString + '100',
      yearMonth: '2015-04',
    }, function(err, m) {
      verifyTheDefaultRows(err, m);
      verifyExtraRows(err, m);
      m.id.should.have.properties(ID_1);
      m.id.should.not.have.property('id');
      m.should.have.properties({yearMonth: '2015-04'});
      done();
    });
  });

  it('find by secondary key without primary key', function(done) {
    CASS_SORTABLE.find(
      {where: {yearMonth: '2015-04'}}, function(err, rows) {
        should.not.exist(err);
        rows.should.have.length(2); // num DESC
        rows[0].str.should.eql(cassTestString + '50');
        rows[1].str.should.eql(rows[0].str);
        rows[0].num.should.be.eql(60);
        rows[1].num.should.be.eql(50);
        done();
      });
  });

  var ID_2, savedTuple, savedTimeUuid;
  var origTupleArray = ['USA', 'California', 'San Francisco', 'Market St.'];
  var origTuple = cassandra.types.Tuple.fromArray(origTupleArray);
  var origTimeUuid = cassandra.types.TimeUuid.now();

  it('create tutple and timeuuid', function(done) {
    CASS_TUPLE_TIME.create({
      tuple: origTuple.values(),
      time: origTimeUuid.toString(),
      str: cassTestString,
      num: cassTestNum,
    }, function(err, m) {
      verifyTheDefaultRows(err, m);
      var mTupleArray = m.tuple.values();
      mTupleArray.should.be.instanceof(Array);
      mTupleArray.should.containDeep(origTupleArray);
      origTupleArray.should.containDeep(mTupleArray);
      savedTuple = m.tuple;
      var mTimeString = m.time.toString();
      mTimeString.should.be.instanceof(String);
      m.time.should.eql(origTimeUuid);
      savedTimeUuid = m.time;
      ID_2 = m.id;
      done();
    });
  });

  it('find by id tuple and timeuuid', function(done) {
    CASS_TUPLE_TIME.findById(savedTimeUuid, function(err, m) {
      should.not.exist(err);
      m.time.should.eql(savedTimeUuid);
      m.tuple.should.eql(savedTuple);
      done();
    });
  });

  it('find by tuple and timeuuid', function(done) {
    CASS_TUPLE_TIME.find(
      {where: {time: savedTimeUuid}}, function(err, m) {
      should.not.exist(err);
      m[0].time.should.eql(savedTimeUuid);
      m[0].tuple.should.eql(savedTuple);
      done();
    });
  });

  var targetTable = 'CASS_SORTABLE';

  it('discoverPrimaryKeys', function(done) {
    db.discoverPrimaryKeys(targetTable, {}, function(err, data) {
      if (err) return done(err);
      data.should.exist;
      data.should.eql(['patNum','patBool','patStr','str','num']);
      done();
    });
  });

  it('discoverSchemas', function(done) {
    db.discoverSchemas(targetTable, {}, function(err, data) {
      if (err) return done(err);
      var tableName = 'test.' + targetTable;
      var existingTableNames = Object.keys(data);
      existingTableNames.should.lengthOf(1);
      existingTableNames[0].should.eql(tableName);
      done();
    });
  });

  it('discoverSchema', function(done) {
    db.discoverSchema(targetTable, {}, function(err, data) {
      if (err) return done(err);
      data.properties.should.exist;
      var targetPropertyNames = ['num', 'patbool', 'patnum', 'patstr', 'str', 'yearmonth'];
      Object.keys(data.properties).should.eql(targetPropertyNames);
      done();
    });
  });

  it('discoverForeignKeys', function(done) {
    (function() {
      db.discoverForeignKeys(targetTable, {}, function(err, data) {
      });
    }).should.throw('self.buildQueryForeignKeys is not a function');
    done();
  });

  it('discoverExportedForeignKeys', function(done) {
    (function() {
      db.discoverExportedForeignKeys(targetTable, {}, function(err, data) {
      });
    }).should.throw('self.buildQueryExportedForeignKeys is not a function');
    done();
  });

  it('discoverAndBuildModels', function(done) {
    db.discoverAndBuildModels(targetTable, {}, function(err, data) {
      if (err) return done(err);
      // console.log('============== discoverAndBuildModels:', err, data);
      done();
    });
  });

  it('discoverModelDefinitions', function(done) {
    db.discoverModelDefinitions({}, function(err, data) {
      if (err) return done(err);
      var columnFamilyName = 'test';
      var tableNamesInTheColuymnFamily = [];
      data.forEach(function(row) {
        if (row.owner === columnFamilyName) {
          tableNamesInTheColuymnFamily.push(row.name);
        }
      })
      targetTable.should.oneOf(tableNamesInTheColuymnFamily);
      done(err);
    });
  });

  it('discoverModelProperties', function(done) {
    var targetColumnNames = ['num', 'patBool', 'patNum', 'patStr', 'str', 'yearMonth'];
    db.discoverModelProperties(targetTable, {}, function(err, data) {
      if (err) return done(err);
      data.forEach(function(row, ix) {
        row.owner.should.eql('test');
        row.name.should.eql(targetTable);
        row.columnName.should.eql(targetColumnNames[ix]);
      })
      done();
    });
  });

});

var allInfo, members, teams; // models

var viewSchemas = {
  members: {
    member: String, // clustering key: 2
    team: {type: String, id: true},
    registered: Boolean, // clustering key: 1
    zipCode: Number,
    },
  teams: {
    league: {type: String, id: true},
    team: String, // clustering key: 1
    member: String, // unused but required because it's a primary key of the base table
    },
}

var membersData = [ // registered, member, zipCode, team
  [true, 'John', 98001, 'Green'],
  [true, 'Kate', 98002, 'Red'],
  [false, 'Mary', 98003, 'Blue'],
  [true, 'Bob', 98004, 'Blue'],
  [false, 'Peter', 98005, 'Yellow'],
];

var teamsData = [ // team, league, member
  ['Blue', 'North', ''],
  ['Green', 'North', ''],
  ['Red', 'South', ''],
  ['Yellow', 'South', ''],
];

var knownBlueTeamMembers = [{
  member: 'Mary',
  team: 'Blue',
  registered: false,
  zipCode: 98003,
}, {
  member: 'Bob',
  team: 'Blue',
  registered: true,
  zipCode: 98004,
}];

var knownUnregisteredMembers = [{
  member: 'Mary',
  team: 'Blue',
  registered: false,
  zipCode: 98003,
}, {
  member: 'Peter',
  team: 'Yellow',
  registered: false,
  zipCode: 98005,
}];

function createModelFromViewSchema(viewName) {
  var viewNames = Object.keys(viewSchemas);
  if (viewNames.indexOf(viewName) < 0) {
    return null;
  }
  db.createModel(viewName, viewSchemas[viewName]);
  var model = db.getModel(viewName);
  return model;
}

describe('materialized views', function() {
  before(function(done) {
    db = getSchema();
    allInfo = db.define('allInfo', {
      team: {type: String, id: 1},
      league: String,
      member: {type: String, id: 2},
      zipCode: Number,
      registered: Boolean,
      });
    db.connector.execute('DROP MATERIALIZED VIEW IF EXISTS members', function(err){
      if (err) return done(err);
      db.connector.execute('DROP MATERIALIZED VIEW IF EXISTS teams', function(err){
        if (err) return done(err);
        db.automigrate(['allInfo'], function(err) {
          if (err) return done(err);
          members = createModelFromViewSchema('members');
          members.should.exist;
          teams = createModelFromViewSchema('teams');
          teams.should.exist;
          done();
        });
      });
    });
  });

  it('create materialized view - members', function(done) {
    db.connector.execute('CREATE MATERIALIZED VIEW members AS ' +
      'SELECT member, team, registered, "zipCode" FROM "allInfo" ' +
      'WHERE member IS NOT NULL AND team IS NOT NULL AND ' +
      '"zipCode" IS NOT NULL AND registered IS NOT NULL ' +
      'PRIMARY KEY (team, registered, member)', done);
  });

  it('create materialized view - teams', function(done) {
    db.connector.execute('CREATE MATERIALIZED VIEW teams AS ' +
      'SELECT team, league FROM "allInfo" ' +
      'WHERE league IS NOT NULL AND team IS NOT NULL AND member IS NOT NULL ' +
      'PRIMARY KEY (league, team, member)', done);
  });

  it('populate members test data', function(done) {
    var nVerified = 0;
    membersData.forEach(function(member, ix) {
      allInfo.create({
        registered: member[0],
        member: member[1],
        zipCode: member[2],
        team: member[3],
      }, function(err, result) {
        if (err) return done(err);
        result.registered.should.be.eql(membersData[ix][0]);
        result.member.should.be.eql(membersData[ix][1]);
        result.zipCode.should.be.eql(membersData[ix][2]);
        result.team.should.be.eql(membersData[ix][3]);
        if (++nVerified === membersData.length) done();
      });
    });
  });

  it('populate teams test data', function(done) {
    var nVerified = 0;
    teamsData.forEach(function(team, ix) {
      allInfo.create({
        team: team[0],
        league: team[1],
        member: team[2],
      }, function(err, result) {
        if (err) return done(err);
        result.team.should.be.eql(teamsData[ix][0]);
        result.league.should.be.eql(teamsData[ix][1]);
        result.member.should.be.eql(teamsData[ix][2]);
        if (++nVerified === teamsData.length) done();
      });
    });
  });

  it('find members from team', function(done) {
    members.find(
      {where: {team: 'Blue'}}, function(err, rows) {
        if (err) return done(err);
        rows.should.have.length(2);
        rows.forEach(function(row) {
          row.__data.should.be.oneOf(knownBlueTeamMembers);
        });
        done();
      });
  });

  it('find members from registration status', function(done) {
    members.find(
      {where: {registered: false}}, function(err, rows) {
        if (err) return done(err);
        rows.should.have.length(2);
        rows.forEach(function(row) {
          row.__data.should.be.oneOf(knownUnregisteredMembers);
        });
        done();
      });
  });

  it('find members from team and registration status', function(done) {
    members.find(
      {where: {and: [{team: 'Blue'}, {registered: true}]}}, function(err, rows) {
        if (err) return done(err);
        rows.should.have.length(1);
        rows[0].__data.should.be.eql(knownBlueTeamMembers[1]);
        done();
      });
  });

  it('find team from league', function(done) {
    teams.find(
      {where: {league: 'North'}, fields: ['league', 'team']}, function(err, rows) {
        if (err) return done(err);
        rows.should.have.length(2);
        rows.forEach(function(row) {
          row.__data.should.not.have.property('member');
          row.__data.should.have.property('league', 'North');
          row.__data.team.should.be.oneOf(['Blue', 'Green']);
        });
        done();
      });
  });

  it('find league from team', function(done) {
    teams.find(
      {where: {team: 'Green'}, fields: {member: false}}, function(err, rows) {
        if (err) return done(err);
        rows.should.have.length(1);
        rows[0].__data.should.not.have.property('member');
        rows[0].__data.should.have.property('league', 'North');
        rows[0].__data.should.have.property('team', 'Green');
        done();
      });
  });
});
