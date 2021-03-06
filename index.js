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
        let self = this;
        return new Promise((resolve, reject) => {
            this.cache.count()
            .then((count) => {
                if(!count) resolve(false);
                self.cache.readAndDeleteOne(obj)
                .then((obj) => {
                    resolve(obj);
                })
                .catch((err) => {
                    reject(new Error("extract error read and delete cache"));
                });
            })
            .catch((err) => {
                reject(new Error("extract count error"));
            });
        });
    }

    onConnect(resolve, reject) {
        const self = this;
        return (err) => {
            if(err) reject(new Error("mssql connection"));
            self.req = new Request(self.query, self.cbRequest(resolve, reject));
            self.req.on('row', self.onRow());
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
