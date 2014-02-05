mohair = require 'mohair'
q = require 'q'
_ = require 'underscore'

module.exports =

###################################################################################
# fluent

    clone: ->
        Object.create this

    # prototypically inherit from this
    # and set key to value

    fluent: (key, value) ->
        object = this.clone()
        object[key] = value
        object

    # call: (f, args...) ->
    #     f.apply this, args

###################################################################################
# setters

    $mohair: mohair.escape((string) -> "\"#{string}\"")
    $returning: '*'
    $primaryKey: 'id'
    $allowedColumns: []
    $returnFirst: false

    returning: (arg) ->
        this.fluent '$returning', arg
    connection: (arg) ->
        this.fluent '$connection', arg
    allowedColumns: (columns) ->
        this.fluent '$allowedColumns', this.$allowedColumns.concat(columns)
    primaryKey: (arg) ->
        this.fluent '$primaryKey', arg

    table: (arg) ->
        this
            .fluent('$table', arg)
            .fluent '$mohair', this.$mohair.table arg

    returnFirst: (arg = true) ->
        this.fluent '$returnFirst', arg

###################################################################################
# pipelining

    $beforeInsert: []
    $afterInsert: []
    $beforeUpdate: []
    $afterUpdate: []
    $afterSelect: []
    $afterDelete: []

    beforeInsert: (args...) ->
        this.fluent '$beforeInsert', this.$beforeInsert.concat(args)
    afterInsert: (args...) ->
        this.fluent '$afterInsert', this.$afterInsert.concat(args)
    beforeUpdate: (args...) ->
        this.fluent '$beforeUpdate', this.$beforeUpdate.concat(args)
    afterUpdate: (args...) ->
        this.fluent '$afterUpdate', this.$afterUpdate.concat(args)
    # run on the records returned by a delete
    afterDelete: (args...) ->
        this.fluent '$afterUpdate', this.$afterUpdate.concat(args)
    afterSelect: (args...) ->
        this.fluent '$afterSelect', this.$afterSelect.concat(args)

    runPipeline: (pipeline, data) ->
        reducer = (soFar, f) ->
            soFar.then f
        pipeline.reduce reducer, q(data)

###################################################################################
# pass through to mohair

    sql: ->
        this.replacePlaceholders this.$mohair.sql()
    params: ->
        this.$mohair.params()

    raw: (args...) ->
        this.$mohair.raw args...

    where: (args...) ->
        this.fluent '$mohair', this.$mohair.where args...
    join: (args...) ->
        this.fluent '$mohair', this.$mohair.join args...

    select: (args...) ->
        this.fluent '$mohair', this.$mohair.select args...
    limit: (arg) ->
        this.fluent '$mohair', this.$mohair.limit arg
    offset: (arg) ->
        this.fluent '$mohair', this.$mohair.offset arg
    order: (arg) ->
        this.fluent '$mohair', this.$mohair.order arg
    group: (arg) ->
        this.fluent '$mohair', this.$mohair.group arg
    with: (arg) ->
        this.fluent '$mohair', this.$mohair.with arg

###################################################################################
# connection

    getConnection: ->
        unless this.$connection?
            throw new Error "the method you are calling requires a call to connection() before it"
        if 'function' is typeof this.$connection
            this.$connection cb
            return
        setTimeout ->
            cb null, this.$connection

    query: (sql, params) ->
        d = q.defer()
        self.getConnection (err, connection, done) ->
            if err?
                done()?
                d.reject err
                return
            connection.query sql, params, (err, results) ->
                done()?
                if err?
                    d.reject err
                    return
                d.resolve results

        d.promise

###################################################################################
# command

    insert: (dataOrArray) ->
        array = if Array.isArray dataOrArray then dataOrArray else [dataOrArray]
        self.returnFirst().insertMany array

    insertMany: (array) ->
        self = this

        beforeInsert = (data) ->
            self.runPipeline self.$beforeInsert, data

        q.all(array.map beforeInsert).then (processedArray) ->
            cleanArray = processedArray.map self.pickAllowedColumns

            cleanArray.forEach (cleanData) ->
                if Object.keys(cleanData).length is 0
                    return q.reject new Error 'nothing to insert'

            query = self.$mohair.insertMany cleanArray
            sql = self.replacePlaceholders query.sql()

            if self.$returning?
                sql += 'RETURNING ' + self.$returning

            self.query(sql, query.params()).then (results) ->
                self.afterQuery self.$afterInsert, results

    update: (data) ->
        self = this

        self.runPipeline(self.$beforeUpdate, data).then (processedData) ->
            cleanData = self.pickAllowedColumns processedData

            if Object.keys(cleanData).length is 0
                return q.reject new Error 'nothing to update'

            query = self.$mohair.update cleanData
            sql = self.replacePlaceholders query.sql()

            if self.$returning?
                sql += 'RETURNING ' + self.$returning

            self.query(sql, query.params()).then (results) ->
                self.afterQuery self.$afterUpdate, results

    delete: ->
        self = this

        query = self.$mohair.delete()
        sql = self.replacePlaceholders query.sql()

        if self.$returning?
            sql += 'RETURNING ' + self.$returning

        self.query(sql, query.params()).then (results) ->
            self.afterQuery self.$afterDelete, results

###################################################################################
# query

    find: ->
        self = this

        self.query(self.sql(), self.params()).then (results) ->
            self.afterQuery self.$afterSelect, results

    exists: ->
        self = this

        self.query(self.sql(), self.params()).then (results) ->
            results.rows? and results.rows.length isnt 0

###################################################################################
# easy sugar

    first: ->
        this.limit(1)
            .returnFirst()
            .find()

###################################################################################
# util

    replacePlaceholders: (sql) ->
        # replace ?, ?, ... with $1, $2, ...
        index = 1
        sql.replace /\?/g, -> '$' + index++

    pickAllowedColumns: (data) ->
        _.pick data, self.$allowedColumns

    afterQuery: (pipeline, results) ->
        if results.rows?
            processedRows = q.all results.rows.map (row) ->
                self.runPipeline pipeline, row
            if self.$returnFirst
                processedRows[0]
            else
                processedRows
        else
            results
