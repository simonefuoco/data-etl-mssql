const {Connection, Request} = require('tedious');

//TODO - too many promises

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

        this.generator = function * () {

            const next = async () => {
                this.cache.decrementCounter;
                let obj = await this.cache.readOne();
                await this.cache.delete(obj);
                return obj;
            };

            while (!this.cache.isEmpty()) {
                yield next();
            }

        }.bind(this)();
    }

    isReady() {
        return this.ready;
    }

    init() {
        this.conn = new Connection(this.config)
        this.conn.on('connect', (err) => {
            if(err) throw Error("error tedious connection");
            this.req = new Request(this.query, (err, rowCount) => {
                if(err && !rowCount) throw new Error("error mmsql query execution");
                Promise.all(state.promises)
                .then(values => {
                    ready = true;
                })
                .catch(reason => {
                    throw new Error("mssql insert cache failed");
                });
            });
            this.req.on('row', (columns) => {
                let doc = {};
                for (const col of columns) {
                    doc[col.metadata.colName] = col.value;
                }
                this.promises.push(this.cache.create(doc));
            });
            this.conn.execSql(this.req);
        });
    }

    extract() {
        let {value, done} = this.generator.next().value;
        if (done === false) {
            return false;
        } else {
            return value;
        }
    }
}

module.exports.Extractor = Extractor;
