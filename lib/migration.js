// Copyright IBM Corp. 2015,2017. All Rights Reserved.
// Node module: loopback-connector-cassandra
// This file is licensed under the MIT License.
// License text available at https://opensource.org/licenses/MIT

var async = require('async');
var cassandraMAP = require('cassandra-map');
var f = require('util').format;
module.exports = mixinMigration;

function mixinMigration(Cassandra) {
  /*!
   * Discover the properties from a table
   * @param {String} model The model name
   * @param {Function} cb The callback function
   */
  Cassandra.prototype.getTableStatus = function(model, cb) {
    function decoratedCallback(err, data) {
      if (err) {
        console.error(err);
      }
      if (!err) {
        data.forEach(function(field) {
          field.type = mapCassandraDatatypes(field.type, field.length);
        });
      }
      cb(err, data);
    }

    this.execute('SELECT keyspace_name AS "database", table_name AS "table",' +
      'column_name AS "column", type AS "type", kind AS "kind"' +
      ' FROM "system_schema"."columns" WHERE table_name=\'' +
      this.table(model) + '\' ALLOW FILTERING', decoratedCallback);

  }

  /**
   * Perform autoupdate for the given models
   * @param {String[]} [models] A model name or an array of model names. If not present, apply to all models
   * @callback {Function} [callback] The callback function
   * @param {String|Error} err The error string or object
   */
  Cassandra.prototype.autoupdate = function(models, cb) {
    var self = this;
    if ((!cb) && ('function' === typeof models)) {
      cb = models;
      models = undefined;
    }
    // First argument is a model name
    if ('string' === typeof models) {
      models = [models];
    }

    models = models || Object.keys(this._models);

    async.each(models, function(model, done) {
      if (!(model in self._models)) {
        return process.nextTick(function() {
          done(new Error('Model not found: ' + model));
        });
      }
      self.getTableStatus(model, function(err, fields) {
        if (!err && fields.length) {
          self.alterTable(model, fields, done);
        } else {
          self.createTable(model, done);
        }
      });
    }, cb);
  };

  /*!
   * Check if the models exist
   * @param {String[]} [models] A model name or an array of model names. If not present, apply to all models
   * @param {Function} [cb] The callback function
   */
  Cassandra.prototype.isActual = function(models, cb) {
    var self = this;

    if ((!cb) && ('function' === typeof models)) {
      cb = models;
      models = undefined;
    }
    // First argument is a model name
    if ('string' === typeof models) {
      models = [models];
    }

    models = models || Object.keys(this._models);

    var changes = [];
    async.each(models, function(model, done) {
      self.getTableStatus(model, function(err, fields) {
        changes = changes.concat(self.getAddModifyColumns(model, fields));
        changes = changes.concat(self.getDropColumns(model, fields));
        done(err);
      });
    }, function done(err) {
      if (err) {
        return cb && cb(err);
      }
      var actual = (changes.length === 0);
      cb && cb(null, actual);
    });
  };

  /*!
   * Alter the table for the given model
   * @param {String} model The model name
   * @param {Object[]} actualFields Actual columns in the table
   * @param {Function} [cb] The callback function
   */
  Cassandra.prototype.alterTable = function(model, actualFields, cb) {
    var self = this;
    var pendingChanges = self.getAddModifyColumns(model, actualFields);
    if (pendingChanges.length > 0) {
      self.applySqlChanges(model, pendingChanges, function(err, results) {
        var dropColumns = self.getDropColumns(model, actualFields);
        if (dropColumns.length > 0) {
          self.applySqlChanges(model, dropColumns, cb);
        } else {
          cb && cb(err, results);
        }
      });
    } else {
      var dropColumns = self.getDropColumns(model, actualFields);
      if (dropColumns.length > 0) {
        self.applySqlChanges(model, dropColumns, cb);
      } else {
        cb && process.nextTick(cb.bind(null, null, []));
      }
    }
  };

  Cassandra.prototype.getAddModifyColumns = function(model, actualFields) {
    var sql = [];
    var self = this;
    sql = sql.concat(self.getColumnsToAdd(model, actualFields));
    var drops = self.getPropertiesToModify(model, actualFields);
    if (drops.length > 0) {
      if (sql.length > 0) {
        sql = sql.concat(', ');
      }
      sql = sql.concat(drops);
    }
    return sql;
  }

  Cassandra.prototype.getDropColumns = function(model, actualFields) {
    var sql = [];
    var self = this;
    sql = sql.concat(self.getColumnsToDrop(model, actualFields));
    return sql;
  }

  Cassandra.prototype.getColumnsToAdd = function(model, actualFields) {
    var self = this;
    var m = self._models[model];
    var propNames = Object.keys(m.properties);
    var sql = [];
    propNames.forEach(function(propName) {
      if (self.id(model, propName)) return;
      var found = self.searchForPropertyInActual(
        model, self.column(model, propName), actualFields);
      if (!found && self.propertyHasNotBeenDeleted(model, propName)) {
        sql.push('ADD ' + self.addPropertyToActual(model, propName));
      }
    });
    if (sql.length > 0) {
      sql = [sql.join(', ')];
    }
    return sql;
  }

  Cassandra.prototype.addPropertyToActual = function(model, propName) {
    var self = this;
    var prop = this.getModelDefinition(model).properties[propName];
    var sqlCommand = self.columnEscaped(model, propName)
      + ' ' + self.columnDataType(model, propName) +
      (self.isNullable(prop) ? "" : " NOT NULL");
    return sqlCommand;
  }

  Cassandra.prototype.searchForPropertyInActual = function(model, propName, actualFields) {
    var self = this;
    var found = false;
    actualFields.forEach(function(f) {
      if (f.column === self.column(model, propName)) {
        found = f;
        return;
      }
    });
    return found;
  }

  Cassandra.prototype.getPropertiesToModify = function(model, actualFields) {
    var self = this;
    var sql = [];
    var m = self._models[model];
    var propNames = Object.keys(m.properties);
    var found;
    propNames.forEach(function(propName) {
      if (self.id(model, propName)) {
        return;
      }
      found = self.searchForPropertyInActual(model, propName, actualFields);
      if (found && self.propertyHasNotBeenDeleted(model, propName)) {
        if (datatypeChanged(propName, found)) {
          sql.push('ALTER ' + self.modifyDatatypeInActual(model, propName));
        }
        if (nullabilityChanged(propName, found)) {
          sql.push('ALTER ' + self.modifyNullabilityInActual(model, propName));
        }
      }
    });

    if (sql.length > 0) {
      sql = [sql.join(', ')];
    }

    return sql;

    function datatypeChanged(propName, oldSettings) {
      var newSettings = m.properties[propName];
      if (!newSettings) {
        return false;
      }
      return oldSettings.type.toUpperCase() !== self.columnDataType(model, propName);
    }

    function nullabilityChanged(propName, oldSettings) {
      var newSettings = m.properties[propName];
      if (!newSettings) {
        return false;
      }
      var changed = false;
      if (oldSettings.nullable === 'YES' && !self.isNullable(newSettings)) {
        changed = true;
      }
      if (oldSettings.nullable === 'NO' && self.isNullable(newSettings)) {
        changed = true;
      }
      return changed;
    }
  }

  Cassandra.prototype.modifyDatatypeInActual = function(model, propName) {
    var self = this;
    var sqlCommand = self.columnEscaped(model, propName) + ' TYPE ' +
      self.columnDataType(model, propName);
    return sqlCommand;
  }

  Cassandra.prototype.modifyNullabilityInActual = function(model, propName) {
    var self = this;
    var prop = this.getPropertyDefinition(model, propName);
    var sqlCommand = self.columnEscaped(model, propName) + ' ';
    if (self.isNullable(prop)) {
      sqlCommand = sqlCommand + "DROP ";
    } else {
      sqlCommand = sqlCommand + "SET ";
    }
    // sqlCommand = sqlCommand + "NOT NULL";
    return sqlCommand;
  }

  Cassandra.prototype.getColumnsToDrop = function(model, actualFields) {
    var self = this;
    var sql = [];
    actualFields.forEach(function(actualField) {
      if (self.idColumn(model) === actualField.column) {
        return;
      }
      if (actualFieldNotPresentInModel(actualField, model)) {
        sql.push('DROP ' + self.escapeName(actualField.column));
      }
    });
    if (sql.length > 0) {
      sql = [sql.join(', ')];
    }
    return sql;

    function actualFieldNotPresentInModel(actualField, model) {
      return !(self.propertyName(model, actualField.column));
    }
  }

  Cassandra.prototype.applySqlChanges = function(model, pendingChanges, cb) {
    var self = this;
    if (pendingChanges.length) {
      var thisQuery = 'ALTER TABLE ' + self.tableEscaped(model);
      var ranOnce = false;
      var columnToDrop = null;
      pendingChanges.forEach(function(change) {
        if (ranOnce) {
          thisQuery = thisQuery + ' ';
        }
        thisQuery = thisQuery + ' ' + change;
        var items = change.split(' ');
        columnToDrop = (items.length === 2 && items[0].toUpperCase() === ('DROP')) ?
          items[1] : null;
        ranOnce = true;
      });
      // thisQuery = thisQuery + ';';
      if (columnToDrop) {
        var dropCQL = 'DROP INDEX IF EXISTS "' + model + '_' + columnToDrop.replace(/\"/g, '') + '_idx"';
        // default secondary index naming: table_name_column_name_idx
        self.execute(dropCQL, function() {
          self.execute(thisQuery, cb);
        })
      } else {
        self.execute(thisQuery, cb);
      }
    }
  }

  /*!
   * Build a list of columns for the given model
   * @param {String} model The model name
   * @returns {String}
   */
  Cassandra.prototype.buildColumnDefinitions =
    Cassandra.prototype.propertiesSQL = function(model) {
      var self = this;
      var sql = [];
      var pks = [];
      // pks = pks.concat(this.idNames(model).map(function(i) {
      //   return self.columnEscaped(model, i);
      // }));
      var pksOrdered = [];
      var ss = this.getConnectorSpecificSettings(model);
      var cks = []; // clustering keys
      var corderby = []; // clustering order by
      if (ss && Array.isArray(ss.clusteringKeys)) {
        ss.clusteringKeys.forEach(function(prop) {
          var parts = [];
          prop.split(' ').forEach(function(part) {
            if (part) parts.push(part);
          });
          cks.push(self.columnEscaped(model, parts[0]));
          corderby.push(self.columnEscaped(model, parts[0]) + ' ' + (parts[1] ? parts[1] : 'ASC'));
        });
      }
      var indexes = [];
      var properties = this.getModelDefinition(model).properties;
      Object.keys(properties).forEach(function(prop) {
        var colName = self.columnEscaped(model, prop);
        var id = properties[prop].id;
        if (id) {
          if (typeof id === 'number') {
            pks.push([colName, id]);
          } else {
            pks.push([colName, Number.MAX_SAFE_INTEGER]);
          }
        }
        if (properties[prop].index) indexes.push(colName);
        sql.push(colName + ' ' + self.buildColumnDefinition(model, prop));
      });
      pks.sort(function(a, b) {return a[1] - b[1];});
      pks.forEach(function(pk) {
        pksOrdered.push(pk[0]);
      })
      if (pksOrdered.length > 0) {
        sql.push('PRIMARY KEY(' + 
          '(' + (pksOrdered.length > 0 ? pksOrdered.join(',') : '') + ')' +
          (cks.length ? ',' + cks.join(',') : '') + ')' +
          (corderby.length ? ') WITH CLUSTERING ORDER BY (' + corderby.join(',') : ''));
      }
      return sql.join(',\n  ');
    };

  /*!
   * Build settings for the model property
   * @param {String} model The model name
   * @param {String} propName The property name
   * @returns {*|string}
   */
  Cassandra.prototype.buildColumnDefinition = function(model, propName) {
    var self = this;
    var modelDef = this.getModelDefinition(model);
    var prop = modelDef.properties[propName];
    if (prop.id && prop.generated) {
      return 'UUID';
    }
    var result = self.columnDataType(model, propName);
    return result;
  };

/**
 * Define foreign key to another model
 * @param {String} className The model name that owns the key
 * @param {String} key Name of key field
 * @param {Function} cb
 */
Cassandra.prototype.defineForeignKey = function(className, key, cb) {
  cb();
  var props = this.getModelDefinition(className).properties;
  props[key].index = true;
};

  /*!
   * Create a table for the given model
   * @param {String} model The model name
   * @param {Function} [cb] The callback function
   */
  Cassandra.prototype.createTable = function(model, cb) {
    var self = this;
    var ksName = self.escapeName(self.dataSource.settings.keyspace);
    var replication = self.dataSource.settings.replication ||  {
      class: 'SimpleStrategy', 
      replication_factor: 3,
    };

    var sqlKeyspace = 'CREATE KEYSPACE IF NOT EXISTS ' + ksName +
      ' WITH REPLICATION = ' + cassandraMAP.stringify(replication);

    self.execute(sqlKeyspace, function(err) {
      if (err) {
        return cb && cb(err);
      }
      self.execute('USE ' + ksName, function(err) {
        if (err) {
          return cb && cb(err);
        }
        var tableName = self.tableEscaped(model);
        var indexCols = [];
        var properties = self.getModelDefinition(model).properties;
        Object.keys(properties).forEach(function(prop) {
          var colName = self.columnEscaped(model, prop);
          if (properties[prop].index) indexCols.push(colName);
        });
        var dropTable = 'DROP TABLE IF EXISTS ' + tableName;
        self.execute(dropTable, function(err) {
          if (err) return cb && cb(err);
          var sqlTable = 'CREATE TABLE ' + tableName + ' (' +
            self.propertiesSQL(model) + ')';
          self.execute(sqlTable, function(err) {
            if (err) return cb && cb(err);
            async.eachSeries(indexCols, function(col, callback) {
              var sqlIndex = 'CREATE INDEX ON ' + tableName + ' (%s)';
              var sql = f(sqlIndex, col);
              self.execute(sql, callback);
            }, cb);
          });
        });
      });
    });
  };

  /*!
   * Get the database-default value for column from given model property
   *
   * @param {String} model The model name
   * @param {String} property The property name
   * @returns {String} The column default value
   */
  Cassandra.prototype.columnDbDefault = function(model, property) {
    return new Error('Default is not supported in Cassandra: ' + model);
  }

  /*!
   * Find the column type for a given model property
   *
   * @param {String} model The model name
   * @param {String} property The property name
   * @returns {String} The column type
   */
  Cassandra.prototype.columnDataType = function(model, property) {
    var columnMetadata = this.columnMetadata(model, property);
    var colType = columnMetadata && columnMetadata.dataType;
    if (colType) {
      colType = colType.toUpperCase();
    }
    var prop = this.getModelDefinition(model).properties[property];
    if (!prop) {
      return null;
    }
    if (colType) {
      return colType;
    }

    switch (prop.type.name) {
      default:
      case 'String':
      case 'JSON':
      case 'Text':
        return 'TEXT';
      case 'Number':
        return 'INT';
      case 'Date':
        return 'TIMESTAMP';
      case 'Timestamp':
        return 'TIMESTAMP';
      case 'GeoPoint':
      case 'Point':
        return 'POINT';
      case 'Boolean':
        return 'BOOLEAN'; // Cassandra doesn't have built-in boolean
      case 'tupleFromArray':
        function buildTupleDataType(tupleProp) {
          var componentTypes = [];
          tupleProp.componentTypes.forEach(function(componentType) {
            var value = componentType;
            if (typeof componentType === 'object' && componentType.type === 'Tuple') {
              value = buildTupleDataType(componentType);
            }
            componentTypes.push(value);
          });
          return 'TUPLE<' + componentTypes.join() + '>';
        }
        return buildTupleDataType(prop);
    }
  };

  function mapCassandraDatatypes(typeName, typeLength) {
    if (typeName.toUpperCase() === 'CHARACTER VARYING' || typeName.toUpperCase() === 'VARCHAR') {
      return typeLength ? 'VARCHAR('+typeLength.toString()+')' : 'VARCHAR(1024)';
    } else {
      return typeName;
    }
  }

  Cassandra.prototype.propertyHasNotBeenDeleted = function(model, propName) {
    return !!this.getModelDefinition(model).properties[propName];
  };
}
