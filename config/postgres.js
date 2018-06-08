module.exports = {
    Username: process.env.DbUsername,
    Password: process.env.DbPassword,
    Host: process.env.DbHost,
    Port: process.env.DbPort,
    Database: process.env.DbName,
    MaxPool: 25,
    ConnectionString: function() {
        console.log(`postgres://${this.Username}:${this.Password}@${this.Host}:${this.Port}/${this.Database}`);
        return `postgres://${this.Username}:${this.Password}@${this.Host}:${this.Port}/${this.Database}`;
    }
}