var async = require('async');
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

    this.execute('SELECT column_name AS "column", data_type AS "type", ' +
      'is_nullable AS "nullable", character_maximum_length as "length"' // , data_default AS "Default"'
      + ' FROM "information_schema"."columns" WHERE table_name=\'' +
      this.table(model) + '\'', decoratedCallback);

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
        sql.push('ADD COLUMN ' + self.addPropertyToActual(model, propName));
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
          sql.push('ALTER COLUMN ' + self.modifyDatatypeInActual(model, propName));
        }
        if (nullabilityChanged(propName, found)) {
          sql.push('ALTER COLUMN' + self.modifyNullabilityInActual(model, propName));
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
    sqlCommand = sqlCommand + "NOT NULL";
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
        sql.push('DROP COLUMN ' + self.escapeName(actualField.column));
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
      pendingChanges.forEach(function(change) {
        if (ranOnce) {
          thisQuery = thisQuery + ' ';
        }
        thisQuery = thisQuery + ' ' + change;
        ranOnce = true;
      });
      // thisQuery = thisQuery + ';';
      self.execute(thisQuery, cb);
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
      var pks = this.idNames(model).map(function(i) {
        return self.columnEscaped(model, i);
      });
      Object.keys(this.getModelDefinition(model).properties).forEach(function(prop) {
        var colName = self.columnEscaped(model, prop);
        sql.push(colName + ' ' + self.buildColumnDefinition(model, prop));
      });
      if (pks.length > 0) {
        sql.push('PRIMARY KEY(' + pks.join(',') + ')');
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
      return 'TIMEUUID';
    }
    var result = self.columnDataType(model, propName);
    if (!self.isNullable(prop)) result = result + ' NOT NULL';

    result += self.columnDbDefault(model, propName);
    return result;
  };

  /*!
   * Create a table for the given model
   * @param {String} model The model name
   * @param {Function} [cb] The callback function
   */
  Cassandra.prototype.createTable = function(model, cb) {
    var self = this;
    var name = self.tableEscaped(model);

    self.execute('CREATE SCHEMA IF NOT EXISTS ' +
      self.escapeName(self.schema(model)) +
      ' WITH REPLICATION = { \'class\' : ' +
      '\'SimpleStrategy\', \'replication_factor\' : 3 }',
      function(err) {
        if (err) {
          return cb && cb(err);
        }
        self.execute('CREATE TABLE ' + name + ' (\n  ' +
          self.propertiesSQL(model) + '\n)', cb);
      });
  };

  Cassandra.prototype.buildIndex = function(model, property) {
    var prop = this.getModelDefinition(model).properties[property];
    var i = prop && prop.index;
    if (!i) {
      return '';
    }
    var type = '';
    var kind = '';
    if (i.type) {
      type = 'USING ' + i.type;
    }
    if (i.kind) {
      kind = i.kind;
    }
    var columnName = this.columnEscaped(model, property);
    if (kind && type) {
      return (kind + ' INDEX ' + columnName + ' (' + columnName + ') ' + type);
    } else {
      (typeof i === 'object' && i.unique && i.unique === true) && (kind = "UNIQUE");
      return (kind + ' INDEX ' + columnName + ' ' + type + ' (' + columnName + ') ');
    }
  };

  Cassandra.prototype.buildIndexes = function(model) {
    var self = this;
    var indexClauses = [];
    var definition = this.getModelDefinition(model);
    var indexes = definition.settings.indexes || {};
    // Build model level indexes
    for (var index in  indexes) {
      var i = indexes[index];
      var type = '';
      var kind = '';
      if (i.type) {
        type = 'USING ' + i.type;
      }
      if (i.kind) {
        kind = i.kind;
      }
      var indexedColumns = [];
      var indexName = this.escapeName(index);
      if (Array.isArray(i.keys)) {
        indexedColumns = i.keys.map(function(key) {
          return self.columnEscaped(model, key);
        });
      }
      var columns = indexedColumns.join(',') || i.columns;
      if (kind && type) {
        indexClauses.push(kind + ' INDEX ' + indexName + ' (' + columns + ') ' + type);
      } else {
        indexClauses.push(kind + ' INDEX ' + type + ' ' + indexName + ' (' + columns + ')');
      }
    }

    // Define index for each of the properties
    for (var p in definition.properties) {
      var propIndex = self.buildIndex(model, p);
      if (propIndex) {
        indexClauses.push(propIndex);
      }
    }
    return indexClauses;
  };

  /*!
   * Get the database-default value for column from given model property
   *
   * @param {String} model The model name
   * @param {String} property The property name
   * @returns {String} The column default value
   */
  Cassandra.prototype.columnDbDefault = function(model, property) {
    var columnMetadata = this.columnMetadata(model, property);
    var colDefault = columnMetadata && columnMetadata.dbDefault;

    return colDefault ? (' DEFAULT ' + columnMetadata.dbDefault) : '';
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
        return 'VARCHAR';
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
    }
  };

  /*!
   * Map postgresql data types to json types
   * @param {String} postgresqlType
   * @param {Number} dataLength
   * @returns {String}
   */
  function postgresqlDataTypeToJSONType(postgresqlType, dataLength) {
    var type = postgresqlType.toUpperCase();
    switch (type) {
      case 'BOOLEAN':
        return 'Boolean';

      /*
       - character varying(n), varchar(n)	variable-length with limit
       - character(n), char(n)	fixed-length, blank padded
       - text	variable unlimited length
       */
      case 'VARCHAR':
      case 'CHARACTER VARYING':
      case 'CHARACTER':
      case 'CHAR':
      case 'TEXT':
        return 'String';

      case 'BYTEA':
        return 'Binary';
      /*
       - smallint	2 bytes	small-range integer	-32768 to +32767
       - integer	4 bytes	typical choice for integer	-2147483648 to +2147483647
       - bigint	8 bytes	large-range integer	-9223372036854775808 to 9223372036854775807
       - decimal	variable	user-specified precision, exact	no limit
       - numeric	variable	user-specified precision, exact	no limit
       - real	4 bytes	variable-precision, inexact	6 decimal digits precision
       - double precision	8 bytes	variable-precision, inexact	15 decimal digits precision
       - serial	4 bytes	autoincrementing integer	1 to 2147483647
       - bigserial	8 bytes	large autoincrementing integer	1 to 9223372036854775807
       */
      case 'SMALLINT':
      case 'INT':
      case 'BIGINT':
      case 'DECIMAL':
      case 'NUMERIC':
      case 'REAL':
      case 'DOUBLE':
      case 'COUNTER':
        return 'Number';

      /*
       - timestamp [ (p) ] [ without time zone ]	8 bytes	both date and time (no time zone)	4713 BC	294276 AD	1 microsecond / 14 digits
       - timestamp [ (p) ] with time zone	8 bytes	both date and time, with time zone	4713 BC	294276 AD	1 microsecond / 14 digits
       - date	4 bytes	date (no time of day)	4713 BC	5874897 AD	1 day
       - time [ (p) ] [ without time zone ]	8 bytes	time of day (no date)	00:00:00	24:00:00	1 microsecond / 14 digits
       - time [ (p) ] with time zone	12 bytes	times of day only, with time zone	00:00:00+1459	24:00:00-1459	1 microsecond / 14 digits
       - interval [ fields ] [ (p) ]	12 bytes	time interval	-178000000 years	178000000 years	1 microsecond / 14 digits
       */
      case 'DATE':
      case 'TIMESTAMP':
      case 'TIME':
        return 'Date';

      case 'POINT':
        return 'GeoPoint';

      default:
        return 'String';
    }
  }

  function mapCassandraDatatypes(typeName, typeLength) {
    if (typeName.toUpperCase() === 'CHARACTER VARYING' || typeName.toUpperCase() === 'VARCHAR') {
      return typeLength ? 'VARCHAR('+typeLength+')' : 'VARCHAR(1024)';
    } else {
      return typeName;
    }
  }

  Cassandra.prototype.propertyHasNotBeenDeleted = function(model, propName) {
    return !!this.getModelDefinition(model).properties[propName];
  };
}
