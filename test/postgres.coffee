mesa = require '../src/postgres'

module.exports =

    'mesa controlled connection':

        'done is called on insert': (test) ->
            test.expect 1

            getConnection = (cb) ->
                process.nextTick ->
                    done = -> test.ok true
                    connection =
                        query: (sql, params, cb) ->
                            cb null, {rows: [{id: 3}]}
                    cb null, connection, done

            userTable = mesa
                .connection(getConnection)
                .table('user')
                .attributes(['name'])

            userTable.insert {name: 'foo'}, (err, id) ->
                throw err if err?
                test.done()

        'done is called on update': (test) ->
            test.expect 1

            getConnection = (cb) ->
                process.nextTick ->
                    done = -> test.ok true
                    connection =
                        query: (sql, params, cb) ->
                            cb()
                    cb null, connection, done

            userTable = mesa
                .connection(getConnection)
                .table('user')
                .attributes(['name'])

            userTable.update {name: 'foo'}, (err) ->
                throw err if err?
                test.done()

        'done is called on delete': (test) ->
            test.expect 1

            getConnection = (cb) ->
                process.nextTick ->
                    done = -> test.ok true
                    connection =
                        query: (sql, params, cb) ->
                            cb()
                    cb null, connection, done

            userTable = mesa
                .connection(getConnection)
                .table('user')

            userTable.delete (err) ->
                throw err if err?
                test.done()

        'done is called on first when a record is returned': (test) ->
            test.expect 1

            getConnection = (cb) ->
                process.nextTick ->
                    done = -> test.ok true
                    connection =
                        query: (sql, params, cb) ->
                            cb null, {rows: [{x: 1}]}
                    cb null, connection, done

            userTable = mesa
                .connection(getConnection)
                .table('user')

            userTable.first (err) ->
                throw err if err?
                test.done()

        'done is called on first when no record is returned': (test) ->
            test.expect 1

            getConnection = (cb) ->
                process.nextTick ->
                    done = -> test.ok true
                    connection =
                        query: (sql, params, cb) ->
                            cb null, {rows: []}
                    cb null, connection, done

            userTable = mesa
                .connection(getConnection)
                .table('user')

            userTable.first (err) ->
                throw err if err?
                test.done()

        'done is called on find when a record is returned': (test) ->
            test.expect 1

            getConnection = (cb) ->
                process.nextTick ->
                    done = -> test.ok true
                    connection =
                        query: (sql, params, cb) ->
                            cb null, {rows: [{x: 1}]}
                    cb null, connection, done

            userTable = mesa
                .connection(getConnection)
                .table('user')

            userTable.find (err) ->
                throw err if err?
                test.done()

        'done is called on find when no records are returned': (test) ->
            test.expect 1

            getConnection = (cb) ->
                process.nextTick ->
                    done = -> test.ok true
                    connection =
                        query: (sql, params, cb) ->
                            cb null, {rows: []}
                    cb null, connection, done

            userTable = mesa
                .connection(getConnection)
                .table('user')

            userTable.find (err) ->
                throw err if err?
                test.done()

        'insert with on conflict': (test) ->

            test.expect 3

            connection =
                query: (sql, params, cb) ->
                    test.equal sql, 'INSERT INTO "user"("name", "email") VALUES ($1, $2) ON CONFLICT DO UPDATE SET ref_count = user.ref_count + 1 RETURNING *'
                    test.deepEqual params, ['foo', 'foo@example.com']
                    cb null, {rows: [{id: 3, name: 'foo', email: 'foo@example.com'}]}

            userTable = mesa
                .connection(connection)
                .table('user')
                .attributes(['name', 'email'])

            userTable
                .primaryKey('my_id')
                .onConflict('DO UPDATE SET ref_count = user.ref_count + 1')
                .returning('*')
                .insert {name: 'foo', email: 'foo@example.com', x: 5}, (err, record) ->
                    throw err if err?
                    test.deepEqual record,
                        id: 3
                        name: 'foo'
                        email: 'foo@example.com'

                    test.done()

