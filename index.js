const {Connection, Request} = require('tedious');
const EventEmitter = require('events');

//TODO - too many promises - paginate result

class Extractor extends EventEmitter
{
    constructor(args) {
        super();
        this.conn = null;
        this.promises = [];
        this.req = null;
        this.config = args.config;
        this.query = args.query;
        this.cache = args.cache;
    }

    init() {
        const self = this;
        return new Promise((resolve, reject) => {
            self.conn = new Connection(self.config);
            self.conn.on('connect', self.onConnect(resolve, reject));
        });
    }

    extract(obj) {
        if (!this.cache.isEmpty) {
            return this.cache.readAndDeleteOne(obj);
        } else {
            return false;
        }
    }

    onConnect(resolve, reject) {
        const self = this;
        return (err) => {
            if(err) reject(new Error("mssql connection"));
            self.req = new Request(self.query, self.cbRequest(resolve, reject));
            self.req.on('row', onRow());
            self.conn.execSql(self.req);
        };
    }

    cbRequest(resolve, reject) {
        const self = this;
        return (err, rowCount) => {
            if(err && !rowCount) reject(new Error("mmsql req"));
            Promise.all(self.promises)
            .then(values => {
                if(self.emit('data-etl-extractor-ready')) {
                    resolve();
                } else {
                    reject(new Error("mssql cache create - no handlers"));
                }
            })
            .catch(reason => {
                reject(new Error("mssql cache create"));
            });
        };
    }

    onRow() {
        const self = this;
        return (columns) => {
            let doc = {};
            for (const col of columns) {
                doc[col.metadata.colName] = col.value;
            }
            self.promises.push(self.cache.createOne(doc));
        }
    }
}

module.exports.Extractor = Extractor;
