const fs           = require('fs');
const path         = require('path');
const shelljs      = require('shelljs');

const winston      = require('winston');

const isProductionEnv = (process.env.OBSERVER_ENV == 'production');

let LOG_ROOT = process.env.OBSERVER_LOGS || "/var/log/ewc/observer";

const actions = ['postgres', 'redis', 'http', 'web3', 'observer'];
let logDirs   = {};
logDirs.Root  = LOG_ROOT;

winston.add(new winston.transports.Console({
    format: winston.format.simple()
}));

let logger = {
    log: function(action, level, message, details = null) {
        if (!logDirs.hasOwnProperty(action)) {
            winston.log("info", `Log action ${action} is undefined, cannot log message`);
            return;
        } 
        if (details != null) {
            this[action].log(level, message, details);
        } else {
            this[action].log(level, message);
        }
    }
};

actions.forEach(function(action) {
    outputDir = path.join(LOG_ROOT, action);
    logDirs[action] = outputDir;
    if (!fs.existsSync(outputDir)) {
        shelljs.mkdir('-p', outputDir);
    }
    logger[action] = winston.createLogger({
        format: winston.format.combine(
            winston.format.timestamp(),
            winston.format.simple()
        ),
        transports: [
            //
            // - Write to all logs with level `info` and below to `info.log` 
            // - Write all logs error (and below) to `error.log`.
            //
            new winston.transports.File({ filename: `${logDirs[action]}/error.log`, level: 'error' }),
            new winston.transports.File({ filename: `${logDirs[action]}/info.log`, level: 'info' })
        ]
    });
});

actions.forEach(function(action) {
    logger[action].add(new winston.transports.Console({
        format: winston.format.simple()
    }));
});

module.exports = logger;