// Copyright IBM Corp. 2017,2019. All Rights Reserved.
// Node module: loopback-connector-cassandra
// This file is licensed under the MIT License.
// License text available at https://opensource.org/licenses/MIT

'use strict';
var g = require('strong-globalize')();

module.exports = mixinDiscovery;

/*!
 * @param {Cassandra} Cassandra connector class
 * @param {Object} cassandra cassandra driver
 */
function mixinDiscovery(Cassandra, cassandra) {
  var async = require('async');

  function paginateSQL(sql, options, cb) {

  }

  /*!
   * Build sql for listing schemas (databases in Cassandra)
   * @params {Object} [options] Options object
   * @returns {String} The SQL statement
   */
  Cassandra.prototype.buildQuerySchemas = function(options) {
    var sql = 'SELECT keyspace_name AS schema FROM system_schema.keyspaces';
    return sql;
  };

  /*!
   * Build sql for listing tables
   * @param options {all: for all owners, owner: for a given owner}
   * @returns {string} The sql statement
   */
  Cassandra.prototype.buildQueryTables = function(options) {
    var schema = options.owner || options.schema;
    var sql = 'SELECT table_name AS name, keyspace_name AS owner FROM system_schema.tables';
    if (schema) sql = sql + ' WHERE keyspace_name = \'' + schema + '\'';
    return sql;
  };

  /*!
   * Build sql for listing views
   * @param options {all: for all owners, owner: for a given owner}
   * @returns {string} The sql statement
   */
  Cassandra.prototype.buildQueryViews = function(options) {
    var schema = options.owner || options.schema;
    var sql = 'SELECT view_name AS name, base_table_name AS "baseTable", keyspace_name AS owner FROM system_schema.views';
    if (schema) sql = sql + ' WHERE keyspace_name = \'' + schema + '\'';
    return sql;
  };

  /**
   * Discover model definitions
   *
   * @param {Object} options Options for discovery
   * @param {Function} [cb] The callback function
   */

  /*!
   * Normalize the arguments
   * @param table string, required
   * @param options object, optional
   * @param cb function, optional
   */
  Cassandra.prototype.getArgs = function(table, options, cb) {
    if ('string' !== typeof table || !table) {
      throw new Error(g.f('{{table}} is a required string argument: %s', table));
    }
    options = options || {};
    if (!cb && 'function' === typeof options) {
      cb = options;
      options = {};
    }
    if (typeof options !== 'object') {
      throw new Error(g.f('{{options}} must be an {{object}}: %s', options));
    }
    return {
      schema: options.owner || options.schema,
      table: table,
      options: options,
      cb: cb,
    };
  };

  /*!
   * Build the sql statement to query columns for a given table
   * @param schema
   * @param table
   * @returns {String} The sql statement
   */
  Cassandra.prototype.buildQueryColumns = function(schema, table) {
    var sql = 'SELECT keyspace_name AS owner, ' +
      'table_name AS name, column_name AS "columnName", ' +
      'type AS "dataType", type AS "columnType" FROM system_schema.columns';
    if (schema) sql = sql + ' WHERE keyspace_name = \'' + schema + '\'';
    if (table) {
      if (schema) sql = sql + ' AND table_name = \'' + table + '\'';
      else sql + ' WHERE table_name = \'' + table + '\'';
    }
    return sql;
  };

  /**
   * Discover primary keys for a given table
   * @param {String} table The table name
   * @param {Object} options The options for discovery
   * @param {Function} [cb] The callback function
   */
  Cassandra.prototype.discoverPrimaryKeys = function(table, options, cb) {
    var self = this;
    var args = self.getArgs(table, options, cb);
    var schema = args.schema;

    if (typeof(self.getDefaultSchema) === 'function' && !schema) {
      schema = self.getDefaultSchema();
    }

    table = args.table;
    options = args.options;
    cb = args.cb;
    var sql = 'SELECT column_name, kind, position FROM system_schema.columns WHERE ';
    sql = sql + (schema ? 'keyspace_name = \'' + schema + '\' AND ' : '') +
      'table_name = \'' + table + '\'';
    self.execute(sql, function(err, result) {
      if (err) return cb(err);
      var keys = [];
      result.forEach(function(row) {
        if (row.kind === 'partition_key' || row.kind === 'clustering') {
          var pos = row.position + 1;
          var multiplier = (row.kind === 'partition_key' ? 1 : 1000);
          keys.push([row.column_name, pos * multiplier]);
        }
      });
      keys.sort(function(a, b) { return a[1] > b[1]; });
      var primaryKeys = [];
      keys.forEach(function(row) {
        primaryKeys.push(row[0]);
      });
      return cb(null, primaryKeys);
    });
  };

  /**
   * Discover primary keys for a given table
   * @param {String} table The table name
   * @param {Object} options The options for discovery
   * @param {Function} [cb] The callback function
   */

  /*!
   * Build the sql statement for querying foreign keys of a given table
   * @param schema
   * @param table
   * @returns {string}
   */
  Cassandra.prototype.buildQueryForeignKeys = notImplemented;

  /**
   * Discover foreign keys for a given table
   * @param schema
   * @param table
   * @returns {string}
   */

  /*!
   * Retrieves a description of the foreign key columns that reference the
   * given table's primary key columns (the foreign keys exported by a table).
   * They are ordered by fkTableOwner, fkTableName, and keySeq.
   * @param schema
   * @param table
   * @returns {string}
   */
  Cassandra.prototype.buildQueryExportedForeignKeys = notImplemented;

  /**
   * Discover foreign keys that reference to the primary key of this table
   * @param schema
   * @param table
   * @returns {string}
   */

  Cassandra.prototype.buildPropertyType = function(columnDefinition, options) {
    var cassandraType = columnDefinition.dataType;
    var columnType = columnDefinition.columnType;

    var type = cassandraType.toUpperCase();
    switch (type) {
      case 'ASCII':
      case 'INET':
      case 'TEXT':
      case 'VARCHAR':
        return 'String';

      case 'BIGINT':
      case 'COUNTER':
      case 'DECIMAL':
      case 'DOUBLE':
      case 'FLOAT':
      case 'INT':
      case 'VARINT':
        return 'Number';

      case 'DATE':
      case 'TIMESTAMP':
      case 'DATETIME':
        return 'Date';

      case 'POINT':
        return 'GeoPoint';

      case 'BOOL':
      case 'BOOLEAN':
        return 'Boolean';

      case 'BLOB':
        return 'Binary';

      case 'LIST':
      case 'SET':
       return 'Array';

      case 'MAP':
        return 'Object';

      case 'UUID':
        return 'Uuid';

      case 'TIMEUUID':
        return 'TimeUuid';

      case 'TUTPLE':
        return 'Tuple';

      default:
        return 'String';
    }
  };

  Cassandra.prototype.getDefaultSchema = function() {
    if (this.dataSource && this.dataSource.settings &&
      this.dataSource.settings.database) {
      return this.dataSource.settings.database;
    }
    return undefined;
  };

  Cassandra.prototype.setDefaultOptions = function(options) {
  };

  Cassandra.prototype.setNullableProperty = function(r) {
  };

  var notImplemented = function() {
    throw new Error(g.f('Not implemented by the {{Cassandra}} connector'));
  }
}
