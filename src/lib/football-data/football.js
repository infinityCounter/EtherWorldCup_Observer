const axios = require('axios');
const XMLHttpRequest = require("xmlhttprequest").XMLHttpRequest;
const authToken = process.env.FOOTBALL_DATA_KEY;

const apiBase = 'http://api.football-data.org/v1/';
const wcId = 467;
const endpoints = Object.freeze({
    competition: `${apiBase}competitions/${wcId}`,
    fixtures: `${apiBase}competitions/${wcId}/fixtures`,
    fixture: `${apiBase}fixtures/`,
    leagueTable: `${apiBase}competitions/${wcId}/leagueTable`,
    teams: `${apiBase}competitions/${wcId}/teams`
});

axios.defaults.headers.get['X-Auth-Token'] = authToken;

const fetch = function (url) {
    var xmlHttp = new XMLHttpRequest();
    xmlHttp.open( "GET", url, false ); // false for synchronous request
    xmlHttp.setRequestHeader('X-Auth-Token', authToken);
    xmlHttp.send( null );
    return xmlHttp.responseText;
};

module.exports = {
    getCompetition: function() {
        return fetch(endpoints.competition);    
    },

    getCompetitionAsync: function(success = function(x){}, error = function(x){}) {
        return axios.get(endpoints.competition);
    },

    getFixtures: function() {
        return fetch(endpoints.fixtures);
    },

    getFixturesAsync: function() {
        return axios.get(endpoints.fixtures);
    },

    getFixture: function(matchId) {
        return fetch(`${endpoints.fixture}${matchId}`);
    },

    getFixtureAsync: function(matchId) {
        return axios(`${endpoints.fixture}${matchId}`);
    },

    getLeagueTable: function() {
        return fetch(endpoints.leagueTable);
    },

    getLeagueTableAsync: function() {
        return axios.get(endpoints.leagueTable);
    },

    getTeams: function() {
        return fetch(endpoints.teams);
    },

    getTeamsAsync: function() {
        return axios.get(endpoints.teams);
    },

    getPlayers: function(team) {
        return fetch(`${apiBase}teams/${team}/players`);
    },

    getPlayersAsync: function(team) {
        return axios.get(`${apiBase}teams/${team}/players`);
    }
};