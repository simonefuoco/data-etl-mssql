const {Connection, Request} = require('tedious');

//TODO - too many promises - paginate result

class Extractor
{
    constructor(args) {
        this.ready = false;
        this.conn = null;
        this.promises = [];
        this.req = null;
        this.config = args.config;
        this.query = args.query;
        this.cache = args.cache;
    }

    get isReady() {
        return this.ready;
    }

    init() {
        const self = this;
        return new Promise((resolve, reject) => {
            self.conn = new Connection(self.config);
            self.conn.on('connect', self.onConnect(resolve, reject));
        });
    }

    extract() {
        let {value, done} = this.generator.next();
        if (!done) {
            return false;
        } else {
            return value;
        }
    }

    * generator() {
        while(!this.cache.isEmpty()) {
            yield this.cache.readAndDeleteOne();
        }
    }

    onConnect(resolve, reject) {
        const self = this;
        return (err) => {
            if(err) reject(new Error("mssql connection"));
            self.req = new Request(self.query, cbRequest(resolve, reject));
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
                ready = true;
                resolve();
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
            self.promises.push(self.cache.create(doc));
        }
    }
}

module.exports.Extractor = Extractor;
