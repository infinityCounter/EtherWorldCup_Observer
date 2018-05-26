/** Globally installed Dependencies */
const fs = require('fs');

/** Locally installed Dependencies */
const winston = require('winston');

/** Local Modules*/
const logger          = require('./lib/logging/logger.js');
const footballData    = require('./lib/football-data/football.js');
const { Broker }      = require('./lib/web3/broker.js');
const { PgClient }    = require('./lib/database/postgres.js');
const { RedisClient } = require('./lib/database/redis.js');

/** Configuration files */
let brokerConfig   = require('../config/broker.js');
let walletConfig   = require('../config/wallet.js');
let postgresConfig = require('../config/postgres.js');
let redisConfig    = require('../config/redis.js');

const REDIS_LAST_BET_PLACED_BLOCK                        = 'lastBetPlacedBlock';
const REDIS_LAST_BET_CANCELLED_BLOCK                     = 'lastBetCancelledBlock';
const REDIS_CONTRACT_TOTAL_BET                           = 'contract:totalBet';
const REDIS_CONTRACT_TOTAL_WON                           = 'contract:totalWon';
const REDIS_CONTRACT_NUM_BETS                            = 'contract:numBets';

let REDIS_KEY_DEFAULTS = {};
REDIS_KEY_DEFAULTS[REDIS_LAST_BET_PLACED_BLOCK] = brokerConfig.StartBlockHeight;
REDIS_KEY_DEFAULTS[REDIS_LAST_BET_CANCELLED_BLOCK] = brokerConfig.StartBlockHeight;
REDIS_KEY_DEFAULTS[REDIS_CONTRACT_TOTAL_BET] = 0;
REDIS_KEY_DEFAULTS[REDIS_CONTRACT_TOTAL_WON] = 0;
REDIS_KEY_DEFAULTS[REDIS_CONTRACT_NUM_BETS] = 0;

let postgres = new PgClient(postgresConfig);
let redis    = new RedisClient(redisConfig.host, redisConfig.port);
let broker   = new Broker(brokerConfig.WSEndpoint,
    brokerConfig.HTTPEndpoint,
    brokerConfig.ABI,
    brokerConfig.ContractAddess,
    brokerConfig.StartBlockHeight,
    walletConfig.Coinbase,
    walletConfig.Mnemonic);

const shutdown = function (status = 0) {
    logger.log('observer', 'info', 'Shutting down observer....Cleaning up connections');
    redis.cleanup();
    postgres.die().then(() => {
        process.exit(status);
    })
    .catch((err) => {
        logger.log('postgres', 'info', 'Postgres failed to exit cleanly ', {
            error: err
        });
        process.exit(1);
    });
};

process.on('SIGTERM', shutdown);
process.on('SIGINT', shutdown);

const panic = function (action, panicInfo) {
    logger.log(action, 'error', "Panic has occured, killing observer daemon: ", panicInfo);
    logger.log('observer', 'error', `Panic caused by action ${action}`);
    shutdown(1);
};

const getRedisMatchKeys = function(matchId) {
    const prefix = `match:${matchId}`;
    return [
        `${prefix}:status`,
        `${prefix}:score`,
        `${prefix}:totalNumBets`,
        `${prefix}:totalTeamA`,
        `${prefix}:totalTeamB`,
        `${prefix}:totalDraw`,
        `${prefix}:numPayoutAttempts`,
    ];
};

function seedRedisMatchKeys(matchId) {
    const keys = getRedisMatchKeys(matchId);
    return Promise.all([
        redis.setKey(keys[0], 'TIMED'),
        redis.setKey(keys[1], '0-0'),
        redis.setKey(keys[2], '0'),
        redis.setKey(keys[3], '0'),
        redis.setKey(keys[4], '0'),
        redis.setKey(keys[5], '0'),
        redis.setKey(keys[6], '0'),
    ]);
}

function seedRedisMatchKeysX(matchId, match) {
    const keys = getRedisMatchKeys(matchId);
    return Promise.all([
        redis.setKey(keys[0], 'TIMED'),
        redis.setKey(keys[1], '0-0'),
        redis.setKey(keys[2], match.NumBets),
        redis.setKey(keys[3], match.TotalTeamABets),
        redis.setKey(keys[4], match.TotalTeamBBets),
        redis.setKey(keys[5], match.TotalDrawBets),
        redis.setKey(keys[6], match.NumPayoutAttempts),
    ]);
}

const getRedisUserkeys = function(address) {
    const prefix = `user:${address}`;
    return [
        `${prefix}:totalBet`,
        `${prefix}:totalWon`,
        `${prefix}:betIds`,
    ];
};

const pushBetToUserRedis = function(bet) {
    const keys = getRedisUserkeys(bet.Address);
}

const getRedisTeamKeys = function(teamId) {
    const prefix = `team:${teamId}`;
    return [
        `${prefix}:totalBet`
    ];
};

const seedRedisKeys = async function() {
    for (var key in REDIS_KEY_DEFAULTS) {
        if (REDIS_KEY_DEFAULTS.hasOwnProperty(key)) {
            const exists = await redis.doesExist(key);
            if (!exists) {
                await redis.setKey(key, REDIS_KEY_DEFAULTS[key]);
            }
        }
    }
}

const setLastBlocks = async function() {
    const tempA = await redis.getKey(REDIS_LAST_BET_PLACED_BLOCK);
    pendingLastBetPlacedBlock = parseInt(tempA);
    const tempB = await redis.getKey(REDIS_LAST_BET_CANCELLED_BLOCK);
    pendingLastBetCancelledBlock = parseInt(tempB);
}

allTeams = [
    {Id: 0, Name: "Russia"}, 
    {Id: 1, Name: "Saudi Arabia"}, 
    {Id: 2, Name: "Egypt"}, 
    {Id: 3, Name: "Uruguay"}, 
    {Id: 4, Name: "Morocco"}, 
    {Id: 5, Name: "Iran"}, 
    {Id: 6, Name: "Portugal"}, 
    {Id: 7, Name: "Spain"}, 
    {Id: 8, Name: "France"}, 
    {Id: 9, Name: "Australia"}, 
    {Id: 10, Name: "Argentina"}, 
    {Id: 11, Name: "Iceland"}, 
    {Id: 12, Name: "Peru"}, 
    {Id: 13, Name: "Denmark"}, 
    {Id: 14, Name: "Croatia"}, 
    {Id: 15, Name: "Nigeria"}, 
    {Id: 16, Name: "Costa Rica"}, 
    {Id: 17, Name: "Serbia"}, 
    {Id: 18, Name: "Germany"}, 
    {Id: 19, Name: "Mexico"}, 
    {Id: 20, Name: "Brazil"}, 
    {Id: 21, Name: "Switzerland"}, 
    {Id: 22, Name: "Sweden"}, 
    {Id: 23, Name: "South Korea"}, 
    {Id: 24, Name: "Belgium"}, 
    {Id: 25, Name: "Panama"}, 
    {Id: 26, Name: "Tunisia"}, 
    {Id: 27, Name: "England"}, 
    {Id: 28, Name: "Poland"}, 
    {Id: 29, Name: "Senegal"}, 
    {Id: 30, Name: "Colombia"}, 
    {Id: 31, Name: "Japan"},
];

// This seads the teams for the tournament, should be called on startup
const seedTeams = async function() {
    return new Promise((resolve, reject) => {
        postgres.models.Team.findAll()
        // This is what happens when findAll passes or fails
        .then(function(teams) {
            if (teams.length != 32) {
                teams.map(team => team.destroy());
                postgres.teams = allTeams;
                return postgres.saveTeams()
            }
        })
        .catch((err) => {
            logger.log('postgres', 'error', 'Unable to call findAll on teams', {
                error: err
            });
            reject(err);
        })

        // This catches the save teams call in case of a failure
        .then(resolve)
        .catch((err) => {
            logger.log('postgres', 'error', 'Unable to save teams in seedTeams', {
                error: err
            });
            reject(err);
        });
    });
}

// This is a general handler for events on a match
// If a match event is called then the match should be pulled
// and the match updated in the database
// If that observer hasn't finished loading / syncing previous blocks
// just cache it for now and then when it's done save the events gotten in the meantime
let finishedSyncing = false;
let pendingMatchUpdates = [];
let pendingBetPlaces = [];
let pendingLastBetPlacedBlock = 0;
let pendingBetCancels = [];
let pendingLastBetCancelledBlock = 0;

const matchEventHandler = async function(err, result) {
    if (err == null) {
        const matchId = parseInt(result.returnValues.matchId);
        broker.getFullMatchDetails(matchId)
        .then(matchData => {
            const match = broker.parseMatch(matchId, matchData);
            pendingMatchUpdates.push(broker.parseMatch(matchId, matchData));
            if (finishedSyncing) {
                postgres.matches.push(...pendingMatchUpdates);
                pendingMatchUpdates = [];
                postgres.saveMatches().then(() => {
                    if (result.event == 'MatchCreated') {
                        seedRedisMatchKeysX(matchId, match);     
                        autoUpdateMatchUntilEnd(match);                 
                    }
                }).catch(()=>{});
            }
        })
        .catch(err => {
            logger.log('web3', 'error', 'Unable to handle match event', {
                error: err
            });
        });
    } else {
        logger.log('web3', 'error', 'Error occured in web3 match event handler', {
            error: err
        });
    }
}

const seedMatches = async function(err, result) {
    const numStr = await broker.getNumMatches();
    const numMatches = parseInt(numStr);
    let promises = [...Array(numMatches)].map((_, idx) => {
        return broker.getFullMatchDetails(idx)
    });
    const matchSets = await Promise.all(promises);
    const matches = matchSets.map((matchData, idx) => {
        return broker.parseMatch(idx, matchData);
    });
    postgres.matches.push(...matches);
    await postgres.saveMatches();
    await matches.map(match => seedRedisMatchKeysX(match.Id, match));
    matches.map(autoUpdateMatchUntilEnd);
}

const parseFixture = function(raw) {
    return JSON.parse(raw).fixture;
}

const updateMatchCache = async function(matchId) {
    let matchData = null;
    try {
        matchData = await broker.getFullMatchDetails(matchId);
    } catch (err) {
        logger.log('web3', 'error', "Unable to get full match details - updateMatchCache", {
            error: err
        });
        throw err;
    }
    const match = broker.parseMatch(matchId, matchData);
    postgres.matches.push(match);
    try {
        await postgres.saveMatches();
    } catch (err) {
        logger.log('postgres', 'error', "Unable to save match details - updateMatchCache", {
            error: err
        });
        throw err;
    }
    let resp = null;
    try {
        resp = await footballData.getFixtureAsync(match.FixtureId);
    } catch(err) {
        logger.log('http', 'error', "Unable to get football match data - updateMatchCache", {
            error: err
        });
        throw err;
    }
    if (resp.data.hasOwnProperty("error")) {
        logger.log('htttp', 'error', 'Got error when getting fixture');
        throw 'Got error when getting fixture';
    }
    let fixture = resp.data.fixture;
    const homeTeamScore = (fixture.result.goalsHomeTeam != null) ? fixture.result.goalsHomeTeam : 0;
    const awayTeamScore = (fixture.result.goalsAwayTeam != null) ? fixture.result.goalsAwayTeam : 0;
    let keys = getRedisMatchKeys(matchId);
    try {
        await Promise.all([
            redis.setKey(keys[0], fixture.status),
            redis.setKey(keys[1], `${homeTeamScore}-${awayTeamScore}`)
        ]);
    } catch (err) {
        logger.log('redis', 'error', 'Unable to set redis match key - updateMatchCache', {
            error: err
        })
        throw err;
    }
    return {
        Data: match,
        Fixture: fixture,
    };
};

const secondsToMilli = function(seconds) {
    return seconds * 1000;
};

const hours = function(num) {
    return secondsToMilli(num * 3600);
};

const minutes = function(num) {
    return secondsToMilli(num * 60);
};

const seconds = function(num) {
    return secondsToMilli(num);
}

// This function will update a match's info in postgres
// and in the redis cache including it's fixture
// starting 15 or less minutes before a match
const autoUpdateMatchUntilEnd = function(match) {
    let startTime = (match.StartTime - minutes(15) - Date.now());
    if (startTime < 0) {
        startTime = 0;
    }
    const update = async function(match) {
        console.log(`Gonna update cache for match ${match.Id}`)
        try {
            let updated = await updateMatchCache(match.Id);
            match = updated.Data;
        } catch (_)  {
            logger.log('observer', 'error', `Was not able to update match cache in autoupdater ${match.Id}`, {
                error: ""
            });
        }
        // If it has not been 3 and a half hours since the match started
        // or the match is not cancelled
        // or the winner is not set
        if (Date.now() < match.StartTime + hours(3.5) && !match.Cancelled && match.Winner == 0) {
            setTimeout(() => update(match), minutes(5));
        }
    }
    // we want to run this at least once for all matches
    setTimeout(() => update(match), startTime);
}

let matchUpdateSchedule = new Map();
const scheduleMatchUpdate = function(matchId, time) {
    if (matchUpdateSchedule.has(matchId)) {
        console.log("Update already scheduled")
        return;
    }
    matchUpdateSchedule.set(matchId, true);
    setTimeout(function() {
        updateMatchCache(matchId);
        matchUpdateSchedule.delete(matchId);
    }, time);
}

const updateBetRedisKeys = async function(bet, block) {
    const redisLastBetBlock = await redis.getKey(REDIS_LAST_BET_PLACED_BLOCK);
    if (block <= parseInt(redisLastBetBlock)) {
        return;
    }
    const userKeys = getRedisUserkeys(bet.Address);
    await redis.transaction([
        ['set', REDIS_LAST_BET_PLACED_BLOCK, block],
        ['incr', REDIS_CONTRACT_NUM_BETS],
        ['incrbyfloat', REDIS_CONTRACT_TOTAL_BET, bet.Amount],
        ['incrbyfloat', userKeys[0], bet.Amount],
        ['sadd', userKeys[2], `${bet.Match}:${bet.Id}`]
    ]);
}

const betPlacedEventHandler = async function(err, result) {
    if (err == null) {
        const bet = broker.parseBetPlacedEvent(result);
        const eventBlock = result.blockNumber;
        pendingBetPlaces.push(bet);
        pendingLastBetPlacedBlock = eventBlock;
        if (finishedSyncing) {
            postgres.unpersistedBets.push(...pendingBetPlaces);
            pendingBetPlaces = [];
            postgres.save()
            .then(() => { return updateBetRedisKeys(bet, eventBlock) })
            .then(() => scheduleMatchUpdate(bet.Match, seconds(100)))
            .catch((err) => {
                logger.log('postgres', 'error', 'Error occurred when attempting to save bets in bet placed event handler', {
                    error: err
                });
            })
        }
    } else {
        logger.log('web3', 'error', 'Error occured in web3 bet palced event handler', {
            error: err
        });
    }
}

const finishedSyncingHandler = function() {
    finishedSyncing = true;
    postgres.matches.push(...pendingMatchUpdates);
    postgres.unpersistedBets.push(...pendingBetPlaces);
    postgres.cancelledBets.push(...pendingBetCancels);
    return postgres.save();
}

const main = function() {
    console.log("ALL GOOD!")
}

broker.reconnectHandler = async function() {
    await seedMatches();
}

Promise.all([
    broker.setup(),
    redis.setup(),
    postgres.setup(),
])
.then(seedRedisKeys)
.then(setLastBlocks)

// This is what happens when that promise all resolves
.then(seedTeams)
.then(() => {
    broker.addMatchCreatedEventListener(matchEventHandler);
    broker.addMatchCancelledEventListener(matchEventHandler)
    broker.addMatchFailedAttemptedPayoutReleaseEventListener(matchEventHandler)
    broker.addMatchFailedPayoutReleaseEventListener(matchEventHandler)
    broker.addMatchOverEventListener(matchEventHandler)
})
.then(seedMatches)
.then(() => {
    broker.addBetPlacedEventListener(betPlacedEventHandler);
})
.then(finishedSyncingHandler)
.then(main)
.catch(err => {
    logger.log('observer', 'error', 'Unable to setup one of the essential observer services', {
        error: err
    });
    panic('observer', err);
});
