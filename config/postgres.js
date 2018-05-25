module.exports = {
    Username: 'daniel',
    Password: 'testing',
    Host: '127.0.0.1',
    Port: '5432',
    Database: 'ether_wc_2018',
    MaxPool: 25,
    ConnectionString: function() {
        return `postgres://${this.Username}:${this.Password}@${this.Host}:${this.Port}/${this.Database}`;
    }
}