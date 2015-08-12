/*!
 * Cassandra connector for LoopBack
 */
var cassandra = require('cassandra-driver');
var SqlConnector = require('loopback-connector').SqlConnector;
var ParameterizedSQL = SqlConnector.ParameterizedSQL;
var util = require('util');
var debug = require('debug')('loopback:connector:cassandra');

/**
 *
 * Initialize the Cassandra connector against the given data source
 *
 * @param {DataSource} dataSource The loopback-datasource-juggler dataSource
 * @callback {Function} [callback] The callback function
 * @param {String|Error} err The error string or object
 * @header Cassandra.initialize(dataSource, [callback])
 */
exports.initialize = function initializeDataSource(dataSource, callback) {
  if (!cassandra) {
    return;
  }

  var dbSettings = dataSource.settings || {};

  // Use host/hostname as a shortcut for contactPoints
  if (!dbSettings.contactPoints) {
    var host = dbSettings.host || dbSettings.hostname;
    if (typeof host === 'string') {
      dbSettings.contactPoints = [host];
    }
  }

  // Use user/password as a shortcut for authProvider
  if (!dbSettings.authProvider) {
    var user = dbSettings.user || dbSettings.userName;
    var password = dbSettings.password || dbSettings.pass;
    if (user) {
      dbSettings.authProvider =
        new cassandra.auth.PlainTextAuthProvider(user, password);
    }
  }

  dataSource.connector = new Cassandra(cassandra, dbSettings);
  dataSource.connector.dataSource = dataSource;

  if (callback) {
    dataSource.connecting = true;
    dataSource.connector.connect(callback);
  }

};

/**
 * Cassandra connector constructor
 *
 * @param {Cassandra} cassandra Cassandra node.js binding
 * @options {Object} settings An object for the data source settings.
 * See [node-postgres documentation](https://github.com/brianc/node-postgres/wiki/Client#parameters).
 * @property {String} url URL to the database, such as 'postgres://test:mypassword@localhost:5432/devdb'.
 * Other parameters can be defined as query string of the url
 * @property {String} hostname The host name or ip address of the Cassandra DB server
 * @property {Number} port The port number of the Cassandra DB Server
 * @property {String} user The user name
 * @property {String} password The password
 * @property {String} database The database name
 * @property {Boolean} ssl Whether to try SSL/TLS to connect to server
 *
 * @constructor
 */
function Cassandra(cassandra, settings) {
  // this.name = 'cassandra';
  // this._models = {};
  // this.settings = settings;
  this.constructor.super_.call(this, 'cassandra', settings);
  this.clientConfig = settings;
  this.cassandra = cassandra;
  this.settings = settings;
  if (settings.debug) {
    debug('Settings %j', settings);
  }
}

// Inherit from loopback-datasource-juggler BaseSQL
util.inherits(Cassandra, SqlConnector);

Cassandra.prototype.getDefaultSchemaName = function() {
  return 'public';
};

/**
 * Connect to Cassandra
 * @callback {Function} [callback] The callback after the connection is established
 */
Cassandra.prototype.connect = function(callback) {
  var client = this.client = new cassandra.Client(this.clientConfig);
  process.nextTick(function() {
    callback && callback(null, client);
  });
};

/**
 * Execute the sql statement
 *
 * @param {String} sql The CQL statement
 * @param {String[]} params The parameter values for the CQL statement
 * @param {Object} [options] Options object
 * @callback {Function} [callback] The callback after the CQL statement is executed
 * @param {String|Error} err The error string or object
 * @param {Object[]) data The result from the CQL
 */
Cassandra.prototype.executeSQL = function(sql, params, options, callback) {
  var self = this;

  if (params && params.length > 0) {
    debug('CQL: %s\nParameters: %j', sql, params);
  } else {
    debug('CQL: %s', sql);
  }

  this.client.execute(sql, params, {prepare: true}, function(err, data) {
    // if(err) console.error(err);
    if (err && self.settings.debug) {
      debug(err);
    }
    debug("%j", data);
    if (!err) {
      var result = null;
      if (data) {
        switch (data.command) {
          case 'DELETE':
          case 'UPDATE':
            result = {count: data.rowCount};
            break;
          default:
            result = data.rows;
        }
      }
    }
    callback(err ? err : null, result);
  });

};

Cassandra.prototype.buildInsertReturning = function(model, data, options) {
  var idColumnNames = [];
  var idNames = this.idNames(model);
  for (var i = 0, n = idNames.length; i < n; i++) {
    idColumnNames.push(this.columnEscaped(model, idNames[i]));
  }
  return 'RETURNING ' + idColumnNames.join(',');
};

Cassandra.prototype.buildInsertDefaultValues = function(model, data, options) {
  return 'DEFAULT VALUES';
};

// FIXME: [rfeng] The native implementation of upsert only works with
// cassandra 9.1 or later as it requres writable CTE
// See https://github.com/strongloop/loopback-connector-cassandra/issues/27
/**
 * Update if the model instance exists with the same id or create a new instance
 *
 * @param {String} model The model name
 * @param {Object} data The model instance data
 * @callback {Function} [callback] The callback function
 * @param {String|Error} err The error string or object
 * @param {Object} The updated model instance
 */
/*
 Cassandra.prototype.updateOrCreate = function (model, data, callback) {
 var self = this;
 data = self.mapToDB(model, data);
 var props = self._categorizeProperties(model, data);
 var idColumns = props.ids.map(function(key) {
 return self.columnEscaped(model, key); }
 );
 var nonIdsInData = props.nonIdsInData;
 var query = [];
 query.push('WITH update_outcome AS (UPDATE ', self.tableEscaped(model), ' SET ');
 query.push(self.toFields(model, data, false));
 query.push(' WHERE ');
 query.push(idColumns.map(function (key, i) {
 return ((i > 0) ? ' AND ' : ' ') + key + '=$' + (nonIdsInData.length + i + 1);
 }).join(','));
 query.push(' RETURNING ', idColumns.join(','), ')');
 query.push(', insert_outcome AS (INSERT INTO ', self.tableEscaped(model), ' ');
 query.push(self.toFields(model, data, true));
 query.push(' WHERE NOT EXISTS (SELECT * FROM update_outcome) RETURNING ', idColumns.join(','), ')');
 query.push(' SELECT * FROM update_outcome UNION ALL SELECT * FROM insert_outcome');
 var queryParams = [];
 nonIdsInData.forEach(function(key) {
 queryParams.push(data[key]);
 });
 props.ids.forEach(function(key) {
 queryParams.push(data[key] || null);
 });
 var idColName = self.idColumn(model);
 self.query(query.join(''), queryParams, function(err, info) {
 if (err) {
 return callback(err);
 }
 var idValue = null;
 if (info && info[0]) {
 idValue = info[0][idColName];
 }
 callback(err, idValue);
 });
 };
 */

Cassandra.prototype.fromColumnValue = function(prop, val) {
  if (val == null) {
    return val;
  }
  var type = prop.type && prop.type.name;
  if (prop && type === 'Boolean') {
    if (typeof val === 'boolean') {
      return val;
    } else {
      return (val === 'Y' || val === 'y' || val === 'T' ||
      val === 't' || val === '1');
    }
  } else if (prop && type === 'GeoPoint' || type === 'Point') {
    if (typeof val === 'string') {
      // The point format is (x,y)
      var point = val.split(/[\(\)\s,]+/).filter(Boolean);
      return {
        lat: +point[0],
        lng: +point[1]
      };
    } else if (typeof val === 'object' && val !== null) {
      // Now cassandra driver converts point to {x: lng, y: lat}
      return {
        lng: val.x,
        lat: val.y
      };
    } else {
      return val;
    }
  } else {
    return val;
  }
};

/*!
 * Convert to the Database name
 * @param {String} name The name
 * @returns {String} The converted name
 */
Cassandra.prototype.dbName = function(name) {
  if (!name) {
    return name;
  }
  // Cassandra default to lowercase names
  return name.toLowerCase();
};

function escapeIdentifier(str) {
  var escaped = '"';
  for(var i = 0; i < str.length; i++) {
    var c = str[i];
    if(c === '"') {
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
  for(var i = 0; i < str.length; i++) {
    var c = str[i];
    if(c === '\'') {
      escaped += c + c;
    } else if (c === '\\') {
      escaped += c + c;
      hasBackslash = true;
    } else {
      escaped += c;
    }
  }
  escaped += '\'';
  if(hasBackslash === true) {
    escaped = ' E' + escaped;
  }
  return escaped;
}

/*!
 * Escape the name for Cassandra DB
 * @param {String} name The name
 * @returns {String} The escaped name
 */
Cassandra.prototype.escapeName = function(name) {
  if (!name) {
    return name;
  }
  return escapeIdentifier(name);
};

Cassandra.prototype.escapeValue = function(value) {
  if (typeof value === 'string') {
    return escapeLiteral(value);
  }
  if (typeof value === 'number' || typeof value === 'boolean') {
    return value;
  }
  return value;
};

Cassandra.prototype.schema = function(model) {
  var modelDef = this.getModelDefinition(model);
  var meta = modelDef.settings[this.name];
  var s = meta && (meta.schema || meta.keyspace);
  return s || this.settings.keyspace;
};

Cassandra.prototype.tableEscaped = function(model) {
  var schema = this.schema(model);
  if (schema) {
    return this.escapeName(schema) + '.' +
      this.escapeName(this.table(model));
  } else {
    return this.escapeName(this.table(model));
  }
};

function buildLimit(limit, offset) {
  var clause = [];
  if (isNaN(limit)) {
    limit = 0;
  }
  if (isNaN(offset)) {
    offset = 0;
  }
  if (!limit && !offset) {
    return '';
  }
  if (limit) {
    clause.push('LIMIT ' + limit);
  }
  if (offset) {
    clause.push('OFFSET ' + offset);
  }
  return clause.join(' ');
}

Cassandra.prototype.applyPagination = function(model, stmt, filter) {
  var limitClause = buildLimit(filter.limit, filter.offset || filter.skip);
  return stmt.merge(limitClause);
};

Cassandra.prototype.buildExpression = function(columnName, operator,
    operatorValue, propertyDefinition) {
  switch(operator) {
    case 'like':
      return new ParameterizedSQL(columnName + " LIKE ? ESCAPE '\\'",
          [operatorValue]);
    case 'nlike':
      return new ParameterizedSQL(columnName + " NOT LIKE ? ESCAPE '\\'",
          [operatorValue]);
    case 'regexp':
      if (operatorValue.global)
        console.warn('Cassandra regex syntax does not respect the `g` flag');

      if (operatorValue.multiline)
        console.warn('Cassandra regex syntax does not respect the `m` flag');

      var regexOperator = operatorValue.ignoreCase ? ' ~* ?' : ' ~ ?';
      return new ParameterizedSQL(columnName + regexOperator,
          [operatorValue.source]);
    default:
      // invoke the base implementation of `buildExpression`
      return this.invokeSuper('buildExpression', columnName, operator,
          operatorValue, propertyDefinition);
  }
};

/**
 * Disconnect from Cassandra
 * @param {Function} [cb] The callback function
 */
Cassandra.prototype.disconnect = function disconnect(cb) {
  if (this.cassandra) {
    if (this.settings.debug) {
      debug('Disconnecting from ' + this.settings.hostname);
    }
    var cassandra = this.cassandra;
    this.cassandra = null;
    cassandra.end();  // This is sync
  }

  if (cb) {
    process.nextTick(cb);
  }
};

Cassandra.prototype.ping = function(cb) {
  this.execute('SELECT count(*) FROM system.schema_keyspaces', [], cb);
};

Cassandra.prototype.getInsertedId = function(model, info) {
  var idColName = this.idColumn(model);
  var idValue;
  if (info && info[0]) {
    idValue = info[0][idColName];
  }
  return idValue;
};

/*!
 * Convert property name/value to an escaped DB column value
 * @param {Object} prop Property descriptor
 * @param {*} val Property value
 * @returns {*} The escaped value of DB column
 */
Cassandra.prototype.toColumnValue = function(prop, val) {
  if (val == null) {
    // Cassandra complains with NULLs in not null columns
    // If we have an autoincrement value, return DEFAULT instead
    if (prop.autoIncrement || prop.id) {
      return new ParameterizedSQL('DEFAULT');
    }
    else {
      return null;
    }
  }
  if (prop.type === String) {
    return String(val);
  }
  if (prop.type === Number) {
    if (isNaN(val)) {
      // Map NaN to NULL
      return val;
    }
    return val;
  }

  if (prop.type === Date || prop.type.name === 'Timestamp') {
    if (!val.toISOString) {
      val = new Date(val);
    }
    var iso = val.toISOString();

    // Pass in date as UTC and make sure Postgresql stores using UTC timezone
    return new ParameterizedSQL({
      sql: '?::TIMESTAMP WITH TIME ZONE',
      params: [iso]
    });
  }

  // Cassandra support char(1) Y/N
  if (prop.type === Boolean) {
    if (val) {
      return true;
    } else {
      return false;
    }
  }

  if (prop.type.name === 'GeoPoint' || prop.type.name === 'Point') {
    return new ParameterizedSQL({
      sql: 'point(?,?)',
      // Postgres point is point(lng, lat)
      params: [val.lng, val.lat]
    });
  }

  return val;
}

/**
 * Get the place holder in CQL for identifiers, such as ??
 * @param {String} key Optional key, such as 1 or id
 * @returns {String} The place holder
 */
Cassandra.prototype.getPlaceholderForIdentifier = function(key) {
  throw new Error('Placeholder for identifiers is not supported');
};

/**
 * Get the place holder in CQL for values, such as :1 or ?
 * @param {String} key Optional key, such as 1 or id
 * @returns {String} The place holder
 */
Cassandra.prototype.getPlaceholderForValue = function(key) {
  return '$' + key;
};

Cassandra.prototype.getCountForAffectedRows = function(model, info) {
  return info && info.count;
};

/*
Cassandra.prototype.automigrate = function(models, cb) {
  process.nextTick(cb);
}
*/

Cassandra.prototype.autoupdate = function(models, cb) {
  process.nextTick(cb);
}

require('./migration')(Cassandra);

