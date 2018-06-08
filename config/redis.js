module.exports = {
    "devlopment": {
        host: process.env.RedisHost,
        port: process.env.RedisPort,
        pass: process.env.RedisPass,
    },
    "production": {
        host: process.env.RedisHost,
        port: process.env.RedisPort,
        pass: process.env.RedisPass,
    }
}