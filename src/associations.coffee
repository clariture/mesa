_ = require 'underscore'
q = require 'q'
mohair = require 'mohair'

createCriterion = (pk, fk, records) ->
    criterion = {}
    criterion[fk] = _.unique _.pluck records, pk
    return criterion

setOneInclude = (name, pk, fk, records, associated) ->
    records.forEach (record) ->
        record[name] = _.detect associated, (x) -> record[fk] is x[pk]

setManyIncludes = (name, pk, fk, records, associated) ->
    records.forEach (record) ->
        record[name] = _.filter associated, (x) -> record[fk] is x[pk]

module.exports =

    # association
    # -----------

    hasAssociated: (name, associationFun) ->
        self = this
        associations = _.extend {}, self._associations
        associations[name] = associationFun
        self.set '_associations', associations

    _getIncludes: (connection, records, cb) ->
        self = this
        unless self._includes?
            process.nextTick ->
                cb null, records
            return

        keys = Object.keys self._includes

        throw new Error 'empty includes' if keys.length is 0

        keys.forEach (key) =>
            unless self._associations? and self._associations[key]?
                throw new Error "no association: #{key}"

        reducer = (promiseSoFar, key) ->
            promiseSoFar.then ->
                q.nfcall self._associations[key], connection, self._includes[key], records

        promise = keys.reduce reducer, q.resolve()

        promise.thenResolve(records).nodeify cb

    hasOne: (name, model, options) ->
        self = this
        self.hasAssociated name, (connection, subIncludes, records, cb) ->
            if 'function' is typeof model then model = model()
            model = model.connection connection
            model = model.includes subIncludes if 'object' is typeof subIncludes

            throw new Error 'no table set on model' unless self._table?
            throw new Error 'no table set on associated model' unless model._table?

            primaryKey = options?.primaryKey || self._primaryKey
            foreignKey = options?.foreignKey || "#{self._table}_#{self._primaryKey}"

            criterion = createCriterion primaryKey, foreignKey, records

            model.where(criterion).find (err, associated) ->
                return cb err if err?
                setOneInclude name, foreignKey, primaryKey, records, associated
                cb null, records

    hasMany: (name, model, options) ->
        self = this
        self.hasAssociated name, (connection, subIncludes, records, cb) ->
            if 'function' is typeof model then model = model()
            model = model.connection connection
            model = model.includes subIncludes if 'object' is typeof subIncludes

            throw new Error 'no table set on model' unless self._table?
            throw new Error 'no table set on associated model' unless model._table?

            primaryKey = options?.primaryKey || self._primaryKey
            foreignKey = options?.foreignKey || "#{self._table}_#{self._primaryKey}"

            criterion = createCriterion primaryKey, foreignKey, records

            model.where(criterion).find (err, associated) ->
                return cb err if err?
                setManyIncludes name, foreignKey, primaryKey, records, associated
                cb null, records

    belongsTo: (name, model, options) ->
        self = this
        self.hasAssociated name, (connection, subIncludes, records, cb) ->
            if 'function' is typeof model then model = model()
            model = model.connection connection
            model = model.includes subIncludes if 'object' is typeof subIncludes

            throw new Error 'no table set on model' unless self._table?
            throw new Error 'no table set on associated model' unless model._table?

            primaryKey = options?.primaryKey || self._primaryKey
            foreignKey = options?.foreignKey || "#{model._table}_#{model._primaryKey}"

            criterion = createCriterion foreignKey, primaryKey, records

            model.where(criterion).find (err, associated) ->
                return cb err if err?
                setOneInclude name, primaryKey, foreignKey, records, associated
                cb null, records

    hasAndBelongsToMany: (name, model, options) ->
        self = this
        self.hasAssociated name, (connection, subIncludes, records, cb) ->
            if 'function' is typeof model then model = model()
            model = model.connection connection
            model = model.includes subIncludes if 'object' is typeof subIncludes

            throw new Error 'no table set on model' unless self._table?
            throw new Error 'no table set on associated model' unless model._table?

            joinTable = options?.joinTable
            throw new Error 'no join table' unless joinTable?

            primaryKey = options.primaryKey || self._primaryKey
            foreignKey = options.foreignKey || "#{self._table}_#{self._primaryKey}"

            otherPrimaryKey = options.otherPrimaryKey || model._primaryKey
            otherForeignKey = options.otherForeignKey || "#{model._table}_#{model._primaryKey}"

            intersectionCriterion =
                createCriterion primaryKey, foreignKey, records

            query = self._originalMohair.table(joinTable).where(intersectionCriterion)

            sql = self.replacePlaceholders query.sql()

            connection.query sql, query.params(), (err, results) ->
                return cb err if err?

                intersection = results.rows

                records.forEach (record) -> record[name] = []

                if intersection.length is 0
                    return cb null, records

                criterion =
                    createCriterion otherForeignKey, otherPrimaryKey, intersection

                model.where(criterion).find (err, associated) ->
                    return cb err if err?

                    records.forEach (record) ->
                        relevantIntersection = _.filter intersection, (x) ->
                            x[foreignKey] is record[primaryKey]
                        otherPrimaryKeys = _.pluck relevantIntersection, otherForeignKey
                        record[name] = associated.filter (x) ->
                            x[otherPrimaryKey] in otherPrimaryKeys

                    cb null, records
