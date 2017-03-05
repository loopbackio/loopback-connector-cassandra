// Copyright IBM Corp. 2012,2016. All Rights Reserved.
// Node module: loopback-connector-mysql
// This file is licensed under the MIT License.
// License text available at https://opensource.org/licenses/MIT

'use strict';
var g = require('strong-globalize')();

/*!
 * Module dependencies
 */
var cassandra = require('cassandra-driver');

var SqlConnector = require('loopback-connector').SqlConnector;
var ParameterizedSQL = SqlConnector.ParameterizedSQL;
// var EnumFactory = require('./enumFactory').EnumFactory;

var debug = require('debug')('loopback:connector:cassandra');

/**
 * @module loopback-connector-cassandra
 *
 * Initialize the Cassandra connector against the given data source
 *
 * @param {DataSource} dataSource The loopback-datasource-juggler dataSource
 * @param {Function} [callback] The callback function
 */
exports.initialize = function initializeDataSource(dataSource, callback) {
  dataSource.driver = cassandra; // Provide access to the native driver
  dataSource.connector = new Cassandra(dataSource, dataSource.settings);
  dataSource.connector.dataSource = dataSource;

  defineCassandraTypes(dataSource);

  if (callback) {
    if (dataSource.settings.lazyConnect) {
      process.nextTick(function() {
        callback();
      });
    } else {
      dataSource.connector.connect(callback);
    }
  }
};

exports.Cassandra = Cassandra;
exports.driver = cassandra;

function defineCassandraTypes(dataSource) {
  var modelBuilder = dataSource.modelBuilder;
  var defineType = modelBuilder.defineValueType ?
    // loopback-datasource-juggler 2.x
    modelBuilder.defineValueType.bind(modelBuilder) :
    // loopback-datasource-juggler 1.x
    modelBuilder.constructor.registerType.bind(modelBuilder.constructor);

  // The Point type is inherited from jugglingdb mysql adapter.
  // LoopBack uses GeoPoint instead.
  // The Point type can be removed at some point in the future.
  defineType(function Point() {
  });

  defineType(dataSource.driver.types.Uuid.fromString, ['Uuid']);
}

/**
 * @constructor
 * Constructor for Cassandra connector
 * @param {Object} client The node-mysql client object
 */
function Cassandra(dataSource, settings) {
  this.dataSource = dataSource;
  dataSource.setMaxListeners(Infinity);

  SqlConnector.call(this, 'cassandra', settings);
}

require('util').inherits(Cassandra, SqlConnector);

Cassandra.prototype.connect = function(callback) {
  var self = this;
  var options = generateOptions(this.settings);
  var s = self.settings || {};

  if (this.client) {
    if (callback) {
      process.nextTick(function() {
        callback(null, self.client);
      });
    }
  } else {
    this.client = new cassandra.Client(options);
    this.client.connect(function(err) {
      var conn = self.client.controlConnection.connection;
      if (!err) {
        if (self.debug) {
          debug('Cassandra connection is established: ' + self.settings || {});
        }
        // conn.close();
      } else {
        if (self.debug || !callback) {
          console.error('Cassandra connection is failed: ' + self.settings || {}, err);
        }
      }
      callback && callback(err, conn);
    });
  }
};

/**
 * Disconnect from Cassandra
 */
Cassandra.prototype.disconnect = function(cb) {
  if (this.debug) {
    debug('disconnect');
  }
  if (this.client) {
    this.client.shutdown(cb);
  } else {
    process.nextTick(cb);
  }
};

/**
 * Ping Cassandra
 */
Cassandra.prototype.ping = function(cb) {
  this.execute('SELECT now() FROM system.local', cb);
};

/*!
 * Generate the cassandra options from the datasource settings
 */
function generateOptions(settings) {
  var clientOptions = cassandra.defaultOptions();

  settings.hostname = (settings.hostname || settings.host || '127.0.0.1');
  settings.port = (settings.port || 9042);
  settings.database = (settings.keyspace || settings.database || settings.db || 'test');

  if (!settings.contactPoints) {
    clientOptions.contactPoints = [
      settings.hostname,
    ];
  }

  clientOptions.protocolOptions.port = settings.port;
  clientOptions.keyspace = settings.database;

  var username = settings.username || settings.user;
  if (username && settings.password) {
    clientOptions.authProvider = new cassandra.auth.PlainTextAuthProvider(username, settings.password);
  }

  return clientOptions;
}

/**
 * Execute the cql statement
 *
 * @param {String} cql The CQL statement
 * @param {Function} [callback] The callback after the SQL statement is executed
 */
Cassandra.prototype.executeSQL = function(cql, params, options, callback) {
  var self = this;
  var client = this.client;
  var db = this.settings.database;
  if (typeof callback !== 'function') {
    throw new Error(g.f('{{callback}} should be a function'));
  }
  if (debug.enabled) {
    debug('CQL: %s, params: %j', cql, params);
  }

  function myCallback(err, data) {
    if (debug.enabled) {
      if (err) {
        debug('Error: %j', err);
      }
      debug('Data: ', data);
    }
   return callback && callback(err, data ? data.rows : null);
  }

  this.client.execute(cql, params, { prepare : true }, myCallback);
};

Cassandra.prototype.executeCQL = Cassandra.executeSQL;

/**
 * Build a SQL SELECT statement
 * @param {String} model Model name
 * @param {Object} filter Filter object
 * @param {Object} options Options object
 * @returns {ParameterizedSQL} Statement object {sql: ..., params: [...]}
 */
Cassandra.prototype.buildSelect = function(model, filter, options) {
  var selectStmt = new ParameterizedSQL('SELECT ' +
    this.buildColumnNames(model, filter) +
    ' FROM ' + this.tableEscaped(model)
  );

  if (filter) {
    if (filter.where) {
      var whereStmt = this.buildWhere(model, filter.where);
      selectStmt.merge(whereStmt);
    }

    if (filter.order) {
      selectStmt.merge(this.buildOrderBy(model, filter.order));
    }

    if (filter.limit || filter.skip || filter.offset) {
      selectStmt = this.applyPagination(
        model, selectStmt, filter);
    }
  }
  return this.parameterize(selectStmt);
};

Cassandra.prototype.generateMissingDefaults = function(model, data) {
  var props = this.getModelDefinition(model).properties;

  for (var i in props) {
    if (props.hasOwnProperty(i)) {
      var prop = props[i];

      if (prop.hasOwnProperty('generated') && prop.generated) {
        data[i] = null;
      }
    }
  }

  return data;
};

Cassandra.prototype._modifyOrCreate = function(model, data, options, fields, cb) {
  data = this.generateMissingDefaults(model, data);
  var sql = new ParameterizedSQL('INSERT INTO ' + this.tableEscaped(model));
  var columnValues = fields.columnValues;
  var fieldNames = fields.names;
  if (fieldNames.length) {
    sql.merge('(' + fieldNames.join(',') + ')', '');
    var values = ParameterizedSQL.join(columnValues, ',');
    values.sql = 'VALUES(' + values.sql + ')';
    sql.merge(values);
  } else {
    sql.merge(this.buildInsertDefaultValues(model, data, options));
  }

  if (debug.enabled) {
    debug('CQL: %s', sql);
  }

  this.execute(sql.sql, sql.params, options, function(err, info) {
    if (!err && info && info.insertId) {
      data.id = info.insertId;
    }
    var meta = {};
    if (info) {
      meta.isNewInstance = (info.affectedRows === 1);
    }
    cb(err, data, meta);
  });
};

/**
 * Create the data model in Cassandra
 *
 * @param {String} model The model name
 * @param {Object} data The model instance data
 * @param {Object} options Options object
 * @param {Function} [callback] The callback function
 */
Cassandra.prototype.create = function(model, data, options, callback) {
  var self = this;
  data = this.generateMissingDefaults(model, data);
  var stmt = this.buildInsert(model, data, options);
  var idColName = self.idColumn(model);
  data[idColName] = stmt.params[stmt.params.length - 1];
  this.execute(stmt.sql, stmt.params, options, function(err, info) {
    if (err) {
      callback(err);
    } else {
      if (!info) {
        if (info === null || info === undefined) info = [];
        var idColName = self.idColumn(model);
        var idColData = {};
        idColData[idColName] = data[idColName];
        info.push(idColData);
      }
      var insertedId = self.getInsertedId(model, info);
      // FIXME - SETO
      // var sql = new ParameterizedSQL('SELECT * FROM ' +
      //   self.tableEscaped(model) + ' WHERE ' + self.idColumnEscaped(model));
      // var filter = {};
      // filter[idColName] = insertedId;
      // sql = self.applyPagination(model, sql, filter);
      // sql = self.parameterize(sql);
      // self.execute(sql.sql, sql.params, options, callback);
      callback(err, insertedId);
    }
  });
};

/**
 * Replace if the model instance exists with the same id or create a new instance
 *
 * @param {String} model The model name
 * @param {Object} data The model instance data
 * @param {Object} options The options
 * @param {Function} [cb] The callback function
 */
Cassandra.prototype.replaceOrCreate = function(model, data, options, cb) {
  data = this.generateMissingDefaults(model, data);
  var fields = this.buildReplaceFields(model, data);
  this._modifyOrCreate(model, data, options, fields, cb);
};

/**
 * Update if the model instance exists with the same id or create a new instance
 *
 * @param {String} model The model name
 * @param {Object} data The model instance data
 * @param {Object} options The options
 * @param {Function} [cb] The callback function
 */
Cassandra.prototype.save =
Cassandra.prototype.updateOrCreate = function(model, data, options, cb) {
  data = this.generateMissingDefaults(model, data);
  var fields =  this.buildFields(model, data);
  this._modifyOrCreate(model, data, options, fields, cb);
};

function dateToMysql(date) {
  return date.getUTCFullYear() + '-' +
    fillZeros(date.getUTCMonth() + 1) + '-' +
    fillZeros(date.getUTCDate()) + ' ' +
    fillZeros(date.getUTCHours()) + ':' +
    fillZeros(date.getUTCMinutes()) + ':' +
    fillZeros(date.getUTCSeconds());

  function fillZeros(v) {
    return parseInt(v) < 10 ? '0' + v : v;
  }
}

function dateToNumber(val) {
  return (new Date(val)).getTime();
}

Cassandra.prototype.getInsertedId = function(model, info) {
  var idColName = this.idColumn(model);
  var idValue;
  if (info && info[0]) {
    idValue = info[0][idColName];
  }
  return idValue;
};

Cassandra.prototype.generateValueByColumnType = function(type) {
  var Uuid = this.dataSource.driver.types.Uuid.fromString;
  var val = null;
  type = (type.name || type);
  if (type === 'Uuid' || type === Uuid) {
    val = generateCassandraUuidString();
    if (debug.enabled) {
      debug('UUID generated %s', val);
    }
  } else if (type === Date) {
    val = Date.now();
  }
  return val;
};


function generateCassandraUuidString() {
  return cassandra.types.Uuid.random().toString();
}
/*!
 * Convert property name/value to an escaped DB column value
 * @param {Object} prop Property descriptor
 * @param {*} val Property value
 * @returns {*} The escaped value of DB column
 */
Cassandra.prototype.toColumnValue = function(prop, val) {
  var Uuid = this.dataSource.driver.types.Uuid.fromString;

  if (prop.generated && !prop.hasBeenGenerated) {
    val = this.generateValueByColumnType(prop.type);
    prop.hasBeenGenerated = true;

    return this.toColumnValue(prop, val);
  }

  if (val == null) {
    if (prop.autoIncrement || prop.id) {
      val = generateCassandraUuidString();
      return val;
    }
    return null;
  }
  if (!prop) {
    return val;
  }
  if (prop.type === String) {
    return String(val);
  }
  if (prop.type === Number) {
    if (isNaN(val)) {
      // FIXME: [rfeng] Should fail fast?
      return val;
    }
    return val;
  }
  if (prop.type === Uuid || prop.type === 'Uuid' || prop.type.name === 'Uuid') {
    if (isNaN(val)) {
      // FIXME: [rfeng] Should fail fast?
      return val;
    }
    return val;
  }
  if (prop.type === Date) {
    if (!val.toUTCString) {
      val = new Date(val);
    }
    return dateToMysql(val);
    // return dateToNumber(val);
  }
  if (prop.type === Boolean) {
    return !!val;
  }
  if (prop.type.name === 'GeoPoint') {
    return new ParameterizedSQL({
      sql: 'Point(?,?)',
      params: [val.lat, val.lng],
    });
  }
  if (prop.type === Object) {
    return this._serializeObject(val);
  }
  if (typeof prop.type === 'function') {
    return this._serializeObject(val);
  }
  return this._serializeObject(val);
};

Cassandra.prototype._serializeObject = function(obj) {
  var val;
  if (obj && typeof obj.toJSON === 'function') {
    obj = obj.toJSON();
  }
  if (typeof obj !== 'string') {
    val = JSON.stringify(obj);
  } else {
    val = obj;
  }
  return val;
};

/**
 * Get the default data type for ID
 * @param prop Property definition
 * Returns {Function}
 */
Cassandra.prototype.getDefaultIdType = function(prop) {
  var Uuid = this.dataSource.driver.types.Uuid.fromString;
  return Uuid;
};

/*!
 * Convert the data from database column to model property
 * @param {object} Model property descriptor
 * @param {*) val Column value
 * @returns {*} Model property value
 */
Cassandra.prototype.fromColumnValue = function(prop, val) {
  if (val == null) {
    return val;
  }

  var Uuid = this.dataSource.driver.types.Uuid.fromString;

  if (prop) {
    switch (prop.type.name) {
      case 'Number':
        val = Number(val);
        break;
      case 'String':
        val = String(val);
        break;
      case 'Date':

        // Cassandra allows, unless NO_ZERO_DATE is set, dummy date/time entries
        // new Date() will return Invalid Date for those, so we need to handle
        // those separate.
        if (val == '0000-00-00 00:00:00') {
          val = null;
        } else {
          val = new Date(val.toString().replace(/GMT.*$/, 'GMT'));
        }
        break;
      case 'Boolean':
        val = Boolean(val);
        break;
      case 'GeoPoint':
      case 'Point':
        val = {
          lat: val.x,
          lng: val.y,
        };
        break;
      case 'Uuid':
        val = val.toString();
        break;
      case 'List':
      case 'Array':
      case 'Object':
      case 'JSON':
        if (typeof val === 'string') {
          val = JSON.parse(val);
        }
        break;
      default:
        if (prop.type === Uuid) {
          if (typeof val !== 'string') {
            val = val.toString();
          }
        } else if (!Array.isArray(prop.type) && !prop.type.modelName) {
          // Do not convert array and model types
          val = prop.type(val);
        }
        break;
    }
  }
  return val;
};

function escapeIdentifier(str) {
  var escaped = '"';
  for (var i = 0; i < str.length; i++) {
    var c = str[i];
    if (c === '"') {
      escaped += c + c;
    } else {
      escaped += c;
    }
  }
  escaped += '"';
  return escaped;
}

function escapeLiteral(str) {
  var hasBackslash = false;
  var escaped = '\'';
  for (var i = 0; i < str.length; i++) {
    var c = str[i];
    if (c === '\'') {
      escaped += c + c;
    } else if (c === '\\') {
      escaped += c + c;
      hasBackslash = true;
    } else {
      escaped += c;
    }
  }
  escaped += '\'';
  if (hasBackslash === true) {
    escaped = ' E' + escaped;
  }
  return escaped;
}

/**
 * Escape an identifier such as the column name
 * @param {string} name A database identifier
 * @returns {string} The escaped database identifier
 */
Cassandra.prototype.escapeName = function(name) {
  if (!name) {
    return name;
  }
  return escapeIdentifier(name);
};

/**
 * Build the LIMIT clause
 * @param {string} model Model name
 * @param {number} limit The limit
 * @param {number} offset The offset
 * @returns {string} The LIMIT clause
 */
Cassandra.prototype._buildLimit = function(model, limit, offset) {
  if (isNaN(limit)) {
    limit = 0;
  }
  if (isNaN(offset)) {
    offset = 0;
  }
  if (!limit && !offset) {
    return '';
  }
  return 'LIMIT ' + (offset ? (offset + ',' + limit) : limit);
};

Cassandra.prototype.applyPagination = function(model, stmt, filter) {
  var limitClause = this._buildLimit(model, filter.limit,
    filter.offset || filter.skip);
  return stmt.merge(limitClause);
};

/**
 * Get the place holder in SQL for identifiers, such as ??
 * @param {String} key Optional key, such as 1 or id
 * @returns {String} The place holder
 */
Cassandra.prototype.getPlaceholderForIdentifier = function(key) {
  return '??';
};

/**
 * Get the place holder in SQL for values, such as :1 or ?
 * @param {String} key Optional key, such as 1 or id
 * @returns {String} The place holder
 */
Cassandra.prototype.getPlaceholderForValue = function(key) {
  return '?';
};

Cassandra.prototype.getCountForAffectedRows = function(model, info) {
  var affectedRows = info && typeof info.affectedRows === 'number' ?
    info.affectedRows : undefined;
  return affectedRows;
};

Cassandra.prototype.buildExpression = function(columnName, operator, operatorValue,
    propertyDefinition) {
  if (operator === 'regexp') {
    if (operatorValue.ignoreCase)
      g.warn('{{Cassandra}} {{regex}} syntax does not respect the {{`i`}} flag');

    if (operatorValue.global)
      g.warn('{{Cassandra}} {{regex}} syntax does not respect the {{`g`}} flag');

    if (operatorValue.multiline)
      g.warn('{{Cassandra}} {{regex}} syntax does not respect the {{`m`}} flag');

    return new ParameterizedSQL(columnName + ' REGEXP ?',
        [operatorValue.source]);
  }

  // invoke the base implementation of `buildExpression`
  return this.invokeSuper('buildExpression', columnName, operator,
      operatorValue, propertyDefinition);
};

require('./migration')(Cassandra, cassandra);
