// Copyright IBM Corp. 2015,2017. All Rights Reserved.
// Node module: loopback-connector-cassandra
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
var debugFilter = require('debug')('loopback:connector:cassandra:filter');

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
  dataSource.connector = new Cassandra(dataSource.settings);
  dataSource.connector.dataSource = dataSource;
  dataSource.setMaxListeners(Infinity);  

  defineCassandraTypes(dataSource);

  // dataSource.EnumFactory = EnumFactory; // factory for Enums. Note that currently Enums can not be registered.

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

  defineType(function Uuid(val) {
    return (typeof value === 'string' && value.length === 36) ?
      cassandra.types.Uuid.fromString(val) :
      val;
  }, ['Uuid']);

  defineType(function TimeUuid(val) {
    return (typeof value === 'string' && value.length === 36) ?
      cassandra.types.TimeUuid.fromString(val) :
      val;
  }, ['TimeUuid']);

  defineType(function Tuple(val) {
    return (Array.isArray(val)) ?
      cassandra.types.Tuple.fromArray(val) :
      val;
  }, ['Tuple']);
}

/**
 * @constructor
 * Constructor for Cassandra connector
 * @param {Object} settings The cassandra settings object
 */
function Cassandra(settings) {
  SqlConnector.call(this, 'cassandra', settings);
}

require('util').inherits(Cassandra, SqlConnector);

Cassandra.prototype.connect = function(callback) {
  var self = this;
  var options = generateOptions(this.settings);
  var s = self.settings || {};
  var debugEnabled = debug.enabled;

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
        if (debugEnabled) {
          debug('Cassandra connection is established: ' + self.settings || {});
        }
        // conn.close();
      } else {
        if (debugEnabled || !callback) {
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

  clientOptions.socketOptions.connectTimeout = settings.connectTimeout || 30000;
  clientOptions.protocolOptions.readTimeout = settings.readTimeout || 30000;

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
  var debugEnabled = debug.enabled;
  var db = this.settings.database;
  if (typeof callback !== 'function') {
    throw new Error(g.f('{{callback}} should be a function'));
  }
  if (debugEnabled) {
    debug('CQL: %s, params: %j', cql, params);
  }

  function executeCallback(err, data) {
    if (debugEnabled) {
      if (err) {
        debug('Error: %j', err);
      }
      debug('Data: ', data);
    }
    return callback && callback(err, data ? data.rows : null);
  }

  this.client.execute(cql, this.serialize(params), {prepare: true, readTimeout: 30000}, executeCallback);
};

/**
 * Build a CQL SELECT statement
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
    // Cass supports where and limit only
    if (filter.where) {
      var whereStmt = this.buildWhere(model, filter.where);
      selectStmt.merge(whereStmt);
    }
    if (filter.limit) {
      selectStmt = this.applyPagination(model, selectStmt, filter);
    }
    if (debug.enabled) {
      var keys = Object.keys(filter).filter(function(key) {
        return key !== 'where';
      }).filter(function(key) {
        return key !== 'limit';
      });

      if (keys.length > 0) {
        debugFilter(
          '------------->',
          keys,
          '<------------- potentially unsupported filter keys'
        );
      }
    }
  }
  selectStmt.merge('ALLOW FILTERING', ' ');
  return this.parameterize(selectStmt);
};

/**
 * Build a SQL DELETE statement
 * @param {String} model Model name
 * @param {Object} where The where object
 * @param {Object} options Options object
 * @returns {ParameterizedSQL} The SQL DELETE FROM statement
 */
Cassandra.prototype.buildDelete = function(model, where, options) {
  var buildWhere = this.buildWhere(model, where);
  var deleteAll = !buildWhere.sql && buildWhere.params.length === 0;
  var sqlCmd = deleteAll ? 'TRUNCATE TABLE ' : 'DELETE FROM ';
  var deleteStmt = new ParameterizedSQL(sqlCmd + this.tableEscaped(model));
  if (!deleteAll) {
    deleteStmt.merge(buildWhere);
  };
  return deleteStmt;
};

Cassandra.prototype.count = function(model, where, options, cb) {
  if (typeof where === 'function') {
    // Backward compatibility for 1.x style signature:
    // count(model, cb, where)
    var tmp = options;
    cb = where;
    where = tmp;
  }

  var stmt = new ParameterizedSQL('SELECT count(*) as "cnt" FROM ' +
    this.tableEscaped(model));
  stmt = stmt.merge(this.buildWhere(model, where));
  // CASS custom BEGIN
  stmt.sql += ' ALLOW FILTERING';
  // CASS custom END
  stmt = this.parameterize(stmt);
  this.execute(stmt.sql, serialize(stmt.params),
    function(err, res) {
      if (err) {
        return cb(err);
      }
      var c = (res && res[0] && res[0].cnt) || 0;
      // Some drivers return count as a string to contain bigint
      // See https://github.com/brianc/node-postgres/pull/427
      cb(err, Number(c));
    });
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
  var idColName = this.idColumn(model);
  this.execute(sql.sql, this.serialize(sql.params), options, cb);
};

/**
 * Replace all instances that match the where clause with the given data
 * @param {String} model The model name
 * @param {Object} where The where object
 * @param {Object} data The property/value object representing changes
 * to be made
 * @param {Object} options The options object
 * @param {Function} cb The callback function
 */
Cassandra.prototype._replace = function(model, where, data, options, cb) {
  var stmt = this.buildReplace(model, where, data, options);
  this.execute(stmt.sql, serialize(stmt.params), options, function(err, info) {
    return cb(err, info);
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
  if (!data[idColName]) data[idColName] = stmt.params[stmt.params.length - 1];
  this.execute(stmt.sql, serialize(stmt.params), options, function(err, info) {
    if (err) {
      callback(err);
    } else {
      if (!info) {
        if (info === null || info === undefined) info = [];
        var idColData = {};
        idColData[idColName] = data[idColName];
        info.push(idColData);
      }
      var insertedId = self.getInsertedId(model, info);
      callback(err, insertedId);
    }
  });
};

/**
 * Build an array of fields for the replace database operation
 * @param {String} model Model name
 * @param {Object} data Model data object
 * @param {Boolean} excludeIds Exclude id properties or not, default to false
 * @returns {{names: Array, values: Array, properties: Array}}
 */
Cassandra.prototype.buildReplaceFields = function(model, data, excludeIds) {
  var props = this.getModelDefinition(model).properties;
  var keys = Object.keys(props);
  // keys = reorderKeys(keys, this.idColumn(model));
  return this._buildFieldsForKeys(model, data, keys, excludeIds);
};

/**
 * Build an array of fields for the database operation
 * @param {String} model Model name
 * @param {Object} data Model data object
 * @param {Boolean} excludeIds Exclude id properties or not, default to false
 * @returns {{names: Array, values: Array, properties: Array}}
 */
Cassandra.prototype.buildFields = function(model, data, excludeIds) {
  var keys = Object.keys(data);
  // keys = reorderKeys(keys, this.idColumn(model));
  return this._buildFieldsForKeys(model, data, keys, excludeIds);
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
  // data = this.generateMissingDefaults(model, data);
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
  // data = this.generateMissingDefaults(model, data);
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
  if (info && info[info.length - 1]) {
    idValue = info[info.length - 1][idColName];
  }
  return idValue;
};

Cassandra.prototype.generateValueByColumnType = function(type) {
  var Uuid = this.dataSource.driver.types.Uuid.fromString;

  var val = null;
  type = (type.name || type);
  if (type === 'Uuid' || type === Uuid ) {
    val = generateCassandraUuidString();
    if (debug.enabled) {
      debug('UUID generated %s', val);
    }
  } else if (type === Date) {
    val = Date.now();
  }
  return val;
};

function generateCassandraUuid() {
  return cassandra.types.Uuid.random();
}

Cassandra.prototype.generateId = function(client) {
  return generateCassandraTimeUuid();
}

function generateCassandraTimeUuid() {
  return cassandra.types.TimeUuid.now();
}

/*!
 * Convert property name/value to an escaped DB column value
 * @param {Object} prop Property descriptor
 * @param {*} val Property value
 * @returns {*} The escaped value of DB column
 */
Cassandra.prototype.toColumnValue = function(prop, val) {
  var Uuid = this.dataSource.driver.types.Uuid.fromString;

  // TODO, FIXME
  //   [1] original if condition was (prop.generated && !prop.hasBeenGenerated)
  //   [2] then, added !val to make a certain case work.
  //   [3] then, removed it entirely -- still works.
  //
  // if (prop.generated && !prop.hasBeenGenerated && !val) {
  //   val = this.generateValueByColumnType(prop.type);
  //   prop.hasBeenGenerated = true;

  //   return this.toColumnValue(prop, val);
  // }

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
    return val.toString();
  }
  if (prop.type === Date) {
    if (!val.toUTCString) {
      val = new Date(val);
    }
    return dateToMysql(val);
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
  if (prop.type === Buffer) {
    return val;
  }
  if (prop.type === Object) {
    return this._serializeObject(val);
  }
  if (prop.type === Array) {
    if (typeof val === 'string') {
      val = JSON.parse(val);
    }
    return val;
  }
  if (
    prop.type === cassandra.types.Uuid ||
    prop.type === 'Uuid' ||
    prop.type.name === 'Uuid'
  ) {
    return (typeof val === 'string') ?
      new cassandra.types.Uuid.fromString(val) :
      val;
  }
  if (
    prop.type === cassandra.types.TimeUuid ||
    prop.type === 'TimeUuid' ||
    prop.type.name === 'TimeUuid'
  ) {
    return (typeof val === 'string') ?
      new cassandra.types.TimeUuid.fromString(val) :
      val;
  }
  if (
    prop.type === cassandra.types.Tuple ||
    prop.type === 'Tuple' ||
    prop.type.name === 'Tuple'
  ) {
    return (Array.isArray(val)) ?
      new cassandra.types.Tuple.fromArray(val) :
      val;
  }
  if (typeof prop.type === 'function') {
    return this._serializeObject(val);
  }
  if (Array.isArray(prop.type)) {
    if (typeof val === 'string') {
      val = JSON.parse(val);
    }
    return val;
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

Cassandra.prototype.serialize = function(params) {
  var self = this;
  var serialized = [];
  params.forEach(function(param) {
    if (param && !Array.isArray(param) && typeof param === 'object') {
      param = self._serializeObject(param);
    }
    serialized.push(param);
  });
  return serialized;
};


/**
 * Get the default data type for ID
 * @param prop Property definition
 * Returns {Function}
 */
Cassandra.prototype.getDefaultIdType = function(prop) {
  return "Uuid";
};

/*!
 * Convert the data from database column to model property
 * @param {object} Model property descriptor
 * @param {*) val Column value
 * @returns {*} Model property value
 */
Cassandra.prototype.fromColumnValue = function(prop, val) {
  if (!val) {
    return val;
  }

  var Uuid = this.dataSource.driver.types.Uuid.fromString;

  if (prop) {
    if (prop.type.name === 'Number') {
      val = Number(val);
    } else if (prop.type.name === 'String') {
      val = String(val);
    } else if (prop.type.name === 'Date') {
        // Cassandra allows, unless NO_ZERO_DATE is set, dummy date/time entries
        // new Date() will return Invalid Date for those, so we need to handle
        // those separate.
      if (val == '0000-00-00 00:00:00') {
        val = null;
      } else {
        val = new Date(val.toString().replace(/GMT.*$/, 'GMT'));
      }
    } else if (prop.type.name === 'Boolean') {
      val = Boolean(val);
    } else if (
      prop.type.name === 'GeoPoint' ||
      prop.type.name === 'Point'
    ) {
      val = {
        lat: val.x,
        lng: val.y,
      };
    } else if (
      prop.type.name === 'List' ||
      prop.type.name === 'Array' ||
      prop.type.name === 'Object' ||
      prop.type.name === 'JSON'
    ) {
      if (typeof val === 'string') {
        val = JSON.parse(val);
      }
    } else if (
      prop.type === cassandra.types.Uuid ||
      prop.type === 'Uuid' ||
      prop.type.name === 'Uuid' ||
      prop.type === cassandra.types.TimeUuid ||
      prop.type === 'TimeUuid' ||
      prop.type.name === 'TimeUuid'
    ) {
      if (typeof val !== 'string') {
        val = val.toString();
      }
    } else if (
      prop.type === cassandra.types.Tuple ||
      prop.type === 'Tuple' ||
      prop.type.name === 'Tuple'
    ) {
      if (!Array.isArray(val)) {
        val = val.values();
      }
    } else {
      if (
        prop.type === cassandra.types.Uuid ||
        prop.type === 'Uuid' ||
        prop.type.name === 'Uuid' ||
        prop.type === cassandra.types.TimeUuid ||
        prop.type === 'TimeUuid' ||
        prop.type.name === 'TimeUuid'
      ) {
        if (typeof val === 'string') {
          val = val.toString();
        }
      } else if (
        prop.type === cassandra.types.Tuple ||
        prop.type === 'Tuple' ||
        prop.type.name === 'Tuple'
      ) {
        if (!Array.isArray(val)) {
          val = val.values();
        }
      } else if (!Array.isArray(prop.type) && !prop.type.modelName) {
        // Do not convert array and model types
        val = prop.type(val);
      }
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
 * @returns {string} The LIMIT clause
 */
Cassandra.prototype._buildLimit = function(model, limit, offset) {
  if (isNaN(limit)) {
    limit = 0;
  }
  if (!limit) {
    return '';
  }
  return 'LIMIT ' + limit.toString();
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

// Methods explicitly declared as notImplemented
// http://apidocs.strongloop.com/loopback/#persistedmodel

var notImplemented = function() {
  throw new Error(g.f('Not implemented by the {{Cassandra}} connector'));
}

Cassandra.prototype.bulkUpdate = notImplemented;
Cassandra.prototype.changes = notImplemented;
Cassandra.prototype.checkpoint = notImplemented;
Cassandra.prototype.createChangeFilter = notImplemented;
Cassandra.prototype.createChangeStream = notImplemented;
Cassandra.prototype.createUpdates = notImplemented;
Cassandra.prototype.currentCheckpoint = notImplemented;
Cassandra.prototype.delta = notImplemented;
Cassandra.prototype.enableChangeTracking = notImplemented;
Cassandra.prototype.getChangeModel = notImplemented;
Cassandra.prototype.handleChangeError = notImplemented;
Cassandra.prototype.rectifyChange = notImplemented;
Cassandra.prototype.replicate = notImplemented;
Cassandra.prototype.setId = notImplemented;

require('./migration')(Cassandra, cassandra);
