const tedious = require('tedious');

const mssqlImport = (args) => {
    args.promises = [];
    args.conn = new tedious.Connection(args.config);
    conn.on('connect', afterConnect(args));
};

const afterConnect = (args) => {
    return (err) => {
        if(err) throw Error("error tedious connection");
        args.req = new tedious.Request(args.query, afterSqlStream(args));
        req.on('row', forEachRow(args));
        conn.execSql(args.query);
    };
};

const forEachRow = (args) => {
    return (columns) => {
        args.promises.push(save(columns, args));
    };
};

const save = async (columns, args) => {
    let doc = {};
    columns.forEach(col => {
        doc[col.metadata.colName] = col.value;
    });
    args.store.insert(doc);
};



const afterSqlStream = (args) => {
    return (err, rowCount) => {
        if(err && !rowCount) throw new Error("error mmsql query execution");
        await Promise.all(args.promises);
    };
}

module.exports.import = async (args) => {
    await mssqlImport(args);
}