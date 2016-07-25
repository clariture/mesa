_ = require 'underscore'

mesa = require './mesa'

# for now this is just an extension of the core mesa

module.exports = Object.create mesa

# enable postgres escaping
mohair = module.exports._mohair.escape((string) -> "\"#{string}\"")
module.exports._mohair = mohair
module.exports._originalMohair = mohair

module.exports.getConnection = (cb) ->
    connection = this._connection
    unless connection?
        throw new Error "the method you are calling requires a call to connection() before it"
    return connection cb if 'function' is typeof connection
    process.nextTick -> cb null, connection

module.exports.replacePlaceholders = (sql) ->
    # replace ?, ?, ... with $1, $2, ...
    index = 1
    sql.replace /\?/g, -> '$' + index++

module.exports.onConflict = (arg) ->
    throw new Error 'must be a string' unless 'string' is typeof arg
    throw new Error 'must not be the empty string' if arg.length is 0
    this.set '_onConflict', arg

module.exports.returning = (arg) ->
    throw new Error 'must be a string' unless 'string' is typeof arg
    throw new Error 'must not be the empty string' if arg.length is 0
    this.set '_returning', arg

# command
# -------

module.exports.insert = (data, cb) ->
    self = this

    self.assertConnection()
    self.assertTable()
    self.assertAttributes()

    query = self._mohair.insert data

    sql = self.replacePlaceholders query.sql()
    sql += " ON CONFLICT #{self._onConflict}" if self._onConflict?
    sql += " RETURNING #{self._returning}" if self._returning?

    self.getConnection (err, connection, done) ->
        if err?
            done?()
            cb err
            return

        connection.query sql, query.params(), (err, results) ->
            if err?
                done?()
                cb err
                return
            done?()
            return cb null, results unless self._returning?
            return cb null, results.rows[0]
    query


module.exports.insertMany = (array, cb) ->
    self = this

    self.assertConnection()
    self.assertTable()
    self.assertAttributes()

    query = self._mohair.insert array
    
    sql = self.replacePlaceholders query.sql()
    sql += " ON CONFLICT #{self._onConflict}" if self._onConflict?
    sql += " RETURNING #{self._returning}" if self._returning?

    self.getConnection (err, connection, done) ->
        if err?
            done?()
            cb err
            return

        connection.query sql, query.params(), (err, results) ->
            if err?
                done?()
                cb err
                return
            done?()
            return cb null, results unless self._returning?
            return cb null, results.rows
    query

module.exports.delete = (cb) ->
    self = this

    self.assertConnection()
    self.assertTable()

    query = self._mohair.delete()
    sql = self.replacePlaceholders query.sql()

    self.getConnection (err, connection, done) ->
        if err?
            done?()
            cb err
            return

        connection.query sql, query.params(), (err, results) ->
            if err?
                done?()
                cb err
                return
            done?()
            cb null, results
    query

module.exports.update = (updates, cb) ->
    self = this

    self.assertConnection()
    self.assertTable()
    self.assertAttributes()

    query =  self._mohair.update updates

    sql = self.replacePlaceholders query.sql()
    sql += " RETURNING #{self._returning}" if self._returning?

    self.getConnection (err, connection, done) ->
        if err?
            done?()
            cb err
            return

        connection.query sql, query.params(), (err, results) ->
            if err?
                done?()
                cb err
                return
            done?()
            return cb null, results unless self._returning?
            return cb null, results.rows
    query

# query
# -----

module.exports.first = (cb) ->
    self = this

    self.assertConnection()

    sql = self.replacePlaceholders self.sql()
    params = self.params()

    self.getConnection (err, connection, done) ->
        if err?
            done?()
            cb err
            return

        self.hookBeforeFirst? self, connection, sql, params
        connection.query sql, params, (err, results) ->
            self.hookAfterFirst? self, connection, sql, params, err, results

            if err?
                done?()
                cb err
                return

            record = results.rows[0]

            unless record?
                done?()
                cb null, null
                return

            self.hookBeforeGetIncludesForFirst? self, connection, record
            self.connection(connection)._getIncludes [record], (err, withIncludes) ->
                self.hookAfterGetIncludesForFirst? self, connection, err, withIncludes

                if err?
                    done?()
                    cb err
                    return
                done?()
                cb null, withIncludes[0]
    self

module.exports.find = (cb) ->
    self = this

    self.assertConnection()

    sql = self.replacePlaceholders self.sql()
    params = self.params()

    self.getConnection (err, connection, done) ->
        if err?
            done?()
            cb err
            return

        self.hookBeforeFind? self, connection, sql, params
        connection.query sql, params, (err, results) ->
            self.hookAfterFind? self, connection, sql, params, err, results
            if err?
                done?()
                cb err
                return

            records = results.rows

            if records.length is 0
                done?()
                cb null, []
                return

            self.hookBeforeGetIncludesForFind? self, connection, records
            self.connection(connection)._getIncludes records, (err, withIncludes) ->
                self.hookAfterGetIncludesForFind? self, connection, err, withIncludes

                if err?
                    done?()
                    cb err
                    return

                done?()

                cb null, withIncludes
    self

module.exports.exists = (cb) ->
    self = this

    self.assertConnection()

    query =  self._mohair
    sql = self.replacePlaceholders query.sql()

    self.getConnection (err, connection, done) ->
        if err?
            done?()
            cb err
            return

        connection.query sql, query.params(), (err, results) ->
            if err?
                done?()
                cb err
                return

            done?()

            cb null, results.rows.length isnt 0
    query
