const redis = require('redis');

const logger = require('../logging/logger.js');

let instance = 0;

class RedisClient {

    constructor(host, port) {
        this.host = host;
        this.port = port;
        this.instance = instance++;
        this.failedAttempts = 0;
        this.backoff = 0;
        this.timeoutId = null;
    }

    async setup() {
        this.client = await this.getRedisClient(this.host, this.port);
    }

    getRedisClient(host, port) {
        return new Promise((resolve, reject) => {
            let client = redis.createClient(port, host);
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
}

module.exports.RedisClient = RedisClient;