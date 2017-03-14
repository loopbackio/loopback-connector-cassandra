// Copyright IBM Corp. 2013,2016. All Rights Reserved.
// Node module: loopback-datasource-juggler
// This file is licensed under the MIT License.
// License text available at https://opensource.org/licenses/MIT

// This test written in mocha+should.js
'use strict';

/* global getSchema:false */

var should = require('should');

var db = getSchema(), CASS, CASS_SORTABLE;
var ID, ROW;
var cassTestString = 'cassandra test string data';
var cassTestNum = 3;

describe('cassandra custom tests', function() {
  before(function(done) {
    CASS = db.define('CASS', {
      str: String,
      num: Number,
      });
    db.automigrate(['CASS'], function(err) {
      var done = this;
      if (err) {
        return done(err);
      }
      CASS_SORTABLE = db.define('CASS_SORTABLE', {
        str: String,
        num: Number,
        }, {
        cassandra: {
          clusteringKeys: ['str'],
          },     
        });
      db.automigrate(['CASS_SORTABLE'], done);
      }.bind(done));   
  });

  function verifyTheDefaultRow(err, m) {
    should.not.exists(err);
    should.exist(m && m.id);
    should.exist(m && m.str);
    should.exist(m && m.num);
    m.str.should.be.type('string');
    m.str.should.equal(cassTestString);
    m.num.should.be.type('number');
    m.num.should.equal(cassTestNum);    
  }

  // http://apidocs.strongloop.com/loopback/#persistedmodel-create
  it('create', function(done) {
    CASS.create({
      str: cassTestString,
      num: cassTestNum,
    }, function(err, m) {
      verifyTheDefaultRow(err, m);
      ROW = m;
      ID = m.id;
      done();
    });
  });

  // http://apidocs.strongloop.com/loopback/#persistedmodel-findbyid
  it('findOne', function(done) {
    CASS.findOne({where: {id: ID}}, function(err, m) {
      verifyTheDefaultRow(err, m);
      done();
    });
  });

  // http://apidocs.strongloop.com/loopback/#persistedmodel-findbyid
  it('findById', function(done) {
    CASS.findById(ID,
    {}, {}, function(err, m) {
      verifyTheDefaultRow(err, m);
      done();
    });
  });

  // http://apidocs.strongloop.com/loopback/#persistedmodel-prototype-updateattributes
  it('destroyAll with id', function(done) {
    CASS.destroyAll({id: ID},
    function(err, info) {
      should.not.exist(err);
      should.exist(info);
      // however, info.count is not set. :-(
      // info.count.should.be.type('number');
      done();
    });
  });

  // http://apidocs.strongloop.com/loopback/#persistedmodel-create
  it('create', function(done) {
    CASS.create({
      str: cassTestString,
      num: cassTestNum,
    }, function(err, m) {
      verifyTheDefaultRow(err, m);
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
      // should.not.exist(err);
      // should.exist(info);
      // info.count.should.be.type('number');
      // however, CASS requires id to be set
      // if not, use TRUNCATE
      should.exist(err);
      done();
    });
  });

  var ID_1, ID_2;

  it('create', function(done) {
    CASS_SORTABLE.create({
      str: cassTestString + '10',
      num: 10,
    }, function(err, m) {
      should.not.exists(err);
      should.exist(m && m.id);
      should.exist(m && m.str);
      should.exist(m && m.num);
      ID_1 = m.id;
      done();
    });
  });

  it('create', function(done) {
    CASS_SORTABLE.create({
      str: cassTestString + '20',
      num: 20,
    }, function(err, m) {
      should.not.exists(err);
      should.exist(m && m.id);
      should.exist(m && m.str);
      should.exist(m && m.num);
      ID_2 = m.id;
      done();
    });
  });

  it('find and order by str ASC', function(done) {
    CASS_SORTABLE.find(
      {where: {id: ID_1},
      order: 'str ASC'}, function(err, rows) {
        should.not.exist(err);
        rows.should.have.length(1);
        rows[0].num.should.eql(10);
        done();
      });
  });

  it('find and order by str DESC', function(done) {
    CASS_SORTABLE.find(
      {where: {id: ID_1},
      order: 'str DESC'}, function(err, rows) {
        should.not.exist(err);
        rows.should.have.length(1);
        rows[0].num.should.eql(10);
        done();
      });
  });

  it('find with num and order by str ASC', function(done) {
    CASS_SORTABLE.find(
      {where: {and: [{id: ID_1},{num: 10}]},
      order: 'str ASC'}, function(err, rows) {
        should.not.exist(err);
        rows.should.have.length(1);
        rows[0].num.should.eql(10);
        done();
      });
  });

  it('find and order by str DESC', function(done) {
    CASS_SORTABLE.find(
      {where: {and: [{id: ID_1},{num: 10}]},
      order: 'str DESC'}, function(err, rows) {
        should.not.exist(err);
        rows.should.have.length(1);
        rows[0].num.should.eql(10);
        done();
      });
  });

});
