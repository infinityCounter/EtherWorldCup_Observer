const util = require('util');
const redis = require('redis');
const bluebird = require('bluebird');

const logger = require('../logging/logger.js');

bluebird.promisifyAll(redis);
let instance = 0;

class RedisClient {

    constructor(host, port, pass) {
        this.host = host;
        this.port = port;
        this.pass = pass;
        this.instance = instance++;
        this.failedAttempts = 0;
        this.backoff = 0;
        this.timeoutId = null;
    }

    async setup() {
        this.client = await this.getRedisClient(this.host, this.port, this.pass);
    }

    getRedisClient(host, port, pass) {
        return new Promise((resolve, reject) => {
            let options = {};
            if (pass != "") options.password = pass;
            let client = redis.createClient(port, host, options);
            client.on('connect', () => {
                this.backoff = 0;
                this.failedAttempts = 0;
                this.timeoutId = null;
                logger.log('redis', 'info', "Connected to redis successfuly!", {
                    Instance: this.instance
                });
                resolve(client);
            });
            client.on('error', (err) => {
                if (this.failedAttempts > 5) {
                    logger.log('redis', 'error', 'Failed to connect to redis 5 times...Throwing error', {
                        error: err,
                        instance: this.instance
                    });
                    reject(err);
                }
                this.backoff = this.failedAttempts * 2000;
                this.failedAttempts++;
                logger.log('redis', 'error', 'Error occured in redis! Restarting conection', {
                    error: err,
                    instance: this.instance
                });
                this.cleanup();
                this.timeoutId = setTimeout(() => {
                    this.client = this.getRedisClient(host, port);
                }, this.backoff);
            });
        });
    }

    cleanup() {
        if (typeof this.client != 'undefined' && this.client != null) {
            this.client.quit();
            this.client = null;
        }
        if (this.timeoutId != null) {
            global.clearTimeout(this.timeoutId);
        }
    }

    doesExist(key) {
        let that = this;
        return new Promise((resolve, reject) => {
            that.client.get(key, function(err, result) {
                if (result == null) {
                    resolve(false);
                } else {
                    resolve(true);
                }
            });
        });
    }

    setKey(key, value) {
        let that = this;
        return new Promise((resolve, reject) => {
            that.client.set(key, value, function(err, result) {
                if (err != null) {
                    logger.log('redis',
                        'error',
                        'Unable to set redis key ' + key, {
                            error: err
                        }
                    );
                    reject(err);
                } else {
                    resolve(result);
                }
            });
        });
    }

    getKey(key) {
        let that = this;
        return new Promise((resolve, reject) => {
            that.client.get(key, function(err, result) {
                if (err != null) {
                    logger.log('redis',
                        'error',
                        'Unable to get redis key ' + key, {
                            error: err
                        }
                    );
                    reject(err);
                } else {
                    resolve(result);
                }
            });
        });
    }

    sadd(key, value) {
        let that = this;
        return new Promise((resolve, reject) => {
            that.client.sadd(key, value, function(err, result) {
                if (err != null) {
                    logger.log('redis',
                        'error',
                        'Unable to set redis key ' + key, {
                            error: err
                        }
                    );
                    reject(err);
                } else {
                    resolve(result);
                }
            });
        });
    }

    smemebrs(key) {
        let that = this;
        return new Promise((resolve, reject) => {
            that.client.smembers(key, value, function(err, result) {
                if (err != null) {
                    logger.log('redis',
                        'error',
                        'Unable to set redis key ' + key, {
                            error: err
                        }
                    );
                    reject(err);
                } else {
                    resolve(result);
                }
            });
        });
    }

    increment(key) {
        let that = this;
        return new Promise((resolve, reject) => {
            that.client.incr(key, function(err, result) {
                if (err != null) {
                    logger.log('redis',
                        'error',
                        'Unable to increment redis key ' + key, {
                            error: err
                        }
                    );
                    reject(err);
                } else {
                    resolve(result);
                }
            });
        });
    }

    incrementBy(key, incrementer, isFloat = false) {
        let command = (isFloat) ? this.client.incrbyfloat : this.client.incrby;
        return new Promise((resolve, reject) => {
            command(key, function(err, result) {
                if (err != null) {
                    logger.log('redis',
                        'error',
                        'Unable to increment redis key ' + key, {
                            error: err
                        }
                    );
                    reject(err);
                } else {
                    resolve(result);
                }
            });
        });
    }

    transaction(operations) {
        let that = this;
        let tx = that.client.multi();
        return new Promise((resolve, reject) => {
            operations.map(commandSet => {
                if(!util.isArray(commandSet)) {
                    reject("Command Set must be an array");
                }
                let command = commandSet[0];
                if(commandSet.length > 1) {
                    let args = commandSet.slice(1);
                    tx = tx[command](...args); 
                } else {
                    tx = tx[command]();
                }
            });
            tx.exec((err, replies) => {
                if (err != null) {
                    reject(err);
                } else {
                    resolve(replies);
                }
            })
        });
    }
}

module.exports.RedisClient = RedisClient;