/**
 * Microsoft SQL-Server Extractor Module.
 * @author Simone Fuoco <simofstudies@gmail.com>
 * @module module:data-etl-mssql
 * @license MIT
 * @see {@link https://github.com/simonefuoco/data-etl | data-etl}
 * 
 * @requires tedious
 * @see {@link http://tediousjs.github.io/tedious/ | tedious Documentation}
 * @see {@link https://github.com/tediousjs/tedious | tedious Repository}
 * @see {@link https://david-dm.org/tediousjs/tedious | tedious Dependencies} {@link david-dm.org | david-dm.org}}
 * @see {@link https://www.npmjs.com/package/tedious | tedious NPM}
 * @see {@link https://travis-ci.org/tediousjs/tedious | tedious Travis CI Build}
 * @see {@link https://ci.appveyor.com/project/tediousjs/tedious | tedious AppVeyor Build}
 * @see {@link https://tediousjs-slack.herokuapp.com/ | tedious Slack}
 */

const tedious = require('tedious');
const { Readable } = require('stream');

/**
 * @typedef {Object} TediousConnectionConfig
 * @see {@link http://tediousjs.github.io/tedious/api-connection.html | tedious connection documentation}
 */

/**
 * @typedef {Object} NeDBDataStore
 * @see {@link https://github.com/louischatriot/nedb | NeDB Repository and Documentation}
 */

/**
 * @typedef {Object|Array} EventRowColumns
 * @see {@link http://tediousjs.github.io/tedious/api-request.html#event_row| Event Row Columns Documentation}
 */

/**
 * @typedef ExtractorArgs
 * @property {TediousConnectionConfig} config - Tedious configuration object.
 * @property {string} query - SQL Query.
 * @property {NeDBDataStore} store - NeDB DataStore object.
 */

/**
 * @function mssqlImport
 * @private
 * @param {ExtractorArgs} args - Extractor arguments.
 */
const mssqlImport = (args, state) => {
    state.conn = new tedious.Connection(args.config);
    state.conn.on('connect', afterConnect(args, state));
};

/**
 * @function afterConnect
 * @private
 * @param {ExtractorArgs} args - Extractor arguments.
 */
const afterConnect = (args, state) => {
    return (err) => {
        if(err) throw Error("error tedious connection");
        state.req = new tedious.Request(args.query, afterSqlStream(args, state));
        state.req.on('row', forEachRow(args, state));
        state.conn.execSql(state.req);
    };
};

/**
 * @function forEachRow
 * @private
 * @param {ExtractorArgs} args - Extractor arguments.
 */
const forEachRow = (args, state) => {
    return (columns) => {
        save(columns, args, state);
    };
};

/**
 * @function save
 * @private
 * @param {EventRowColumns} columns
 * @param {ExtractorArgs} args - Extractor arguments.
 */
const save = (columns, args, state) => {
    let doc = {};
    columns.forEach(col => {
        doc[col.metadata.colName] = col.value;
    });
    //args.store.insert(doc);
    state.stream.push(doc);
};

/**
 * @function afterSqlStream
 * @private
 * @param {ExtractorArgs} args - Extractor arguments.
 */
const afterSqlStream = (args, state) => {
    return (err, rowCount) => {
        if(err && !rowCount) throw new Error("error mmsql query execution");
        //await Promise.all(state.promises);
        state.stream.push(null);
    };
}

/**
 * Extract data from [Microsoft SQL-Server]{@link https://www.microsoft.com/sql-server}.
 * @function extract
 * @public
 * @param {Object} args - extractor arguments.
 * @return 
 */
module.exports.extract = (args) => {
    let state = {};
    state.stream = new Readable({
        objectMode: true,
        read() {}
    });
    mssqlImport(args, state);
}