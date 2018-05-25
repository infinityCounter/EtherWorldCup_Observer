const express = require('express');
const { Cache } = require('./cache/cache.js');

const app = express();
const cache = new Cache();

// let forceMigrate = false;
// if (process.argv.length > 2) {
//     for (let count = 2; count <= process.argv.length; count++) {
//         if (process.argv[count] == "--forceMigrate") {
//             forceMigrate = true;
//         }
//     }
// }
// pg.setup();

// app.get('/lastUpdate', (req, res) => res.send('' + cache.getLastUpdate()));

// app.get('/competition', (req, res) => res.send(cache.getCompetition()));

// app.get('/table', (req, res) => res.send(cache.getLeagueTable()))

// app.get('/fixtures', (req, res) => res.send(cache.getFixtures()));

// app.get('/fixtures/:id', (req, res) => res.send(cache.getFixture(req.params.id)));

// app.get('/teams', (req, res) => res.send(cache.getTeams()));

// app.get('/teams/:id', (req, res) => res.send(cache.getTeam(req.params.id)));

app.listen(8711);
console.log("Listening on port 8711...")