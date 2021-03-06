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
const env = process.env.OBSERVER_ENV || "development";
console.log(`Starting observer in ${env} environment`);
let brokerConfig   = require('../config/broker.js');
let postgresConfig = require('../config/postgres.js');
let redisConfig    = require('../config/redis.js');

const REDIS_LAST_BET_PLACED_BLOCK       = 'lastBetPlacedBlock';
const REDIS_LAST_BET_CANCELLED_BLOCK    = 'lastBetCancelledBlock';
const REDIS_LAST_BET_CLAIMED_BLOCK      = 'lastBetClaimedBlock';
const REDIS_CONTRACT_TOTAL_BET          = 'contract:totalBet';
const REDIS_CONTRACT_TOTAL_WON          = 'contract:totalWon';
const REDIS_CONTRACT_NUM_BETS           = 'contract:numBets';

let REDIS_KEY_DEFAULTS = {};
REDIS_KEY_DEFAULTS[REDIS_LAST_BET_PLACED_BLOCK]    = brokerConfig[env].StartBlockHeight;
REDIS_KEY_DEFAULTS[REDIS_LAST_BET_CANCELLED_BLOCK] = brokerConfig[env].StartBlockHeight;
REDIS_KEY_DEFAULTS[REDIS_LAST_BET_CLAIMED_BLOCK]   = brokerConfig[env].StartBlockHeight;
REDIS_KEY_DEFAULTS[REDIS_CONTRACT_TOTAL_BET]       = 0;
REDIS_KEY_DEFAULTS[REDIS_CONTRACT_TOTAL_WON]       = 0;
REDIS_KEY_DEFAULTS[REDIS_CONTRACT_NUM_BETS]        = 0;

let postgres = new PgClient(postgresConfig[env]);
let redis    = new RedisClient(redisConfig[env].host, redisConfig[env].port, redisConfig[env].pass);
console.log(`Broker starting for contact at ${brokerConfig[env].ContractAddess} at BlockHeight ${brokerConfig[env].StartBlockHeight}`)
console.log(brokerConfig[env].WSEndpoint,
    brokerConfig[env].HTTPEndpoint,
    brokerConfig[env].ContractAddess,
    brokerConfig[env].StartBlockHeight);
let broker   = new Broker(brokerConfig[env].WSEndpoint,
    brokerConfig[env].HTTPEndpoint,
    brokerConfig[env].ABI,
    brokerConfig[env].ContractAddess,
    brokerConfig[env].StartBlockHeight);

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
    console.log(panicInfo)
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
        `${prefix}:lastMatchMetaUpdate`,
        `${prefix}:lastMatchBetUpdate`
    ];
};

function seedRedisMatchKeys(matchId) {
    const keys = getRedisMatchKeys(matchId);
    const timestamp = Date.now();
    return redis.transaction([
        ['set', keys[0], 'TIMED'],
        ['set', keys[1], '0-0'],
        ['set', keys[2], '0'],
        ['set', keys[3], '0'],
        ['set', keys[4], '0'],
        ['set', keys[5], '0'],
        ['set', keys[6], '0'],
        ['set', keys[7], timestamp],
        ['set', keys[8], timestamp],
    ]);
};

const updateMatchMetaTimestamp = function(matchId) {
    const keys = getRedisMatchKeys(matchId);
    const timestamp = Date.now();
    return redis.transaction([
        ['set', keys[8], timestamp],
    ]);
}

const updateMatchBettingTimestamp = function(matchId) {
    const keys = getRedisMatchKeys(matchId);
    const timestamp = Date.now();
    return redis.transaction([
        ['set', keys[7], timestamp],
    ]);
}

const updateMatchTimestamps = function(matchId) {
    const keys = getRedisMatchKeys(matchId);
    const timestamp = Date.now();
    return redis.transaction([
        ['set', keys[7], timestamp],
        ['set', keys[8], timestamp],
    ]);
}

function seedRedisMatchKeysX(matchId, match) {
    const keys = getRedisMatchKeys(matchId);
    const timestamp = Date.now();
    // Use a transaction to ensure data consistency
    return redis.transaction([
        ['set', keys[0], 'TIMED'],
        ['set', keys[1], '0-0'],
        ['set', keys[2], match.NumBets],
        ['set', keys[3], match.TotalTeamABets],
        ['set', keys[4], match.TotalTeamBBets],
        ['set', keys[5], match.TotalDrawBets],
        ['set', keys[6], match.NumPayoutAttempts],
        ['set', keys[7], timestamp],
        ['set', keys[8], timestamp],
    ]);
};

const getRedisUserkeys = function(address) {
    const prefix = `user:${address}`;
    return [
        `${prefix}:totalBet`,
        `${prefix}:betIds`,
    ];
};

const seedRedisKeys = async function() {
    logger.log('observer', 'info', 'Attempting to seed default redis keys')
    for (var key in REDIS_KEY_DEFAULTS) {
        if (REDIS_KEY_DEFAULTS.hasOwnProperty(key)) {
            const exists = await redis.doesExist(key);
            if (!exists) {
                try {
                    await redis.setKey(key, REDIS_KEY_DEFAULTS[key]);
                } catch (e) {
                    const err = `Failed to seed redis default key ${key}`;
                    logger.log('redis', 'error', err, {
                        error: e
                    });
                    throw e;
                }
            }
        }
    }
};

const setLastBlocks = async function() {
    logger.log('observer', 'info', 'Attempting to load last bet placed and last bet cancelled block numbers from redis');
    try {
        const tempA = await redis.getKey(REDIS_LAST_BET_PLACED_BLOCK);
        pendingLastBetPlacedBlock = parseInt(tempA);
    } catch (e) {
        const err = `Unable to read ${REDIS_LAST_BET_PLACED_BLOCK} from redis`;
        logger.log('redis', 'error', err, {
            error: e
        });
        throw e;
    }
    try {
        const tempB = await redis.getKey(REDIS_LAST_BET_CANCELLED_BLOCK);
        pendingLastBetCancelledBlock = parseInt(tempB);
    } catch (e) {
        const err = `Unable to read ${REDIS_LAST_BET_CANCELLED_BLOCK} from redis`;
        logger.log('redis', 'error', err, {
            error: e
        });
        throw e;
    }
    try {
        const tempC = await redis.getKey(REDIS_LAST_BET_CLAIMED_BLOCK);
        pendingLastBetClaimedBlock = parseInt(tempC);
    } catch (e) {
        const err = `Unable to read ${REDIS_LAST_BET_CLAIMED_BLOCK} from redis`;
        logger.log('redis', 'error', err, {
            error: e
        });
        throw e;
    }
};

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
    logger.log('observer', 'info', 'Attempting to seed teams')
    return new Promise((resolve, reject) => {
        postgres.models.Team.findAll()
        // This is what happens when findAll passes or fails
        .then(function(teams) {
            if (teams.length != 32) {
                logger.log('observer', 'info', 'Number of teams in database not 32');
                teams.map(team => team.destroy());
                postgres.teams = allTeams;
                postgres.saveTeams()
                .then(() => {
                    logger.log('observer', 'info', 'Finished seeding teams')
                    resolve();
                })
                .catch(err => {
                    logger.log('observer', 'error', 'Was unable to seed teams due to postgres save error')
                    reject(err);
                });
            } else {
                resolve();
            }
        })
        .catch((err) => {
            logger.log('postgres', 'error', 'Unable to call findAll on teams', {
                error: err
            });
            reject(err);
        })
    });
}

// This is a general handler for events on a match
// If a match event is called then the match should be pulled
// and the match updated in the database
// If that observer hasn't finished loading / syncing previous blocks
// just cache it for now and then when it's done save the events gotten in the meantime
let finishedSyncing = false;
let pendingMatchUpdates = [];
let pendingMatchCreates = [];
let pendingBetPlaces = [];
let pendingLastBetPlacedBlock = 0;
let pendingBetCancels = [];
let pendingLastBetCancelledBlock = 0;
let pendingBetClaims = [];
let pendingLastBetClaimedBlock = 0;

const matchEventHandler = async function(err, result) {
    if (err == null) {
        logger.log('web3', 'info', `Attemptng to handle match event ${result.event} in block ${result.blockNumber}`)
        const matchId = parseInt(result.returnValues['0']);
        broker.getFullMatchDetails(matchId)
        .then(matchData => {
            const match = broker.parseMatch(matchId, matchData);
            if (result.event == 'MatchCreated') {
                pendingMatchCreates.push(match);
            } else {
                pendingMatchUpdates.push(match);
            }
            if (finishedSyncing) {
                postgres.matches.push(...pendingMatchCreates);
                let extra = pendingMatchCreates;
                pendingMatchCreates = [];
                postgres.saveMatches().then(() => {
                    extra.map(match => {
                        seedRedisMatchKeysX(match.Id, match);
                        autoUpdateMatchUntilEnd(match);
                    });
                    extra = [];
                }).catch(() => {
                    logger.log('postgres', 'error', 'Unable to save match creations, pushing to queue')
                });

                postgres.matches.push(...pendingMatchUpdates);
                let extra2 = pendingMatchUpdates;
                pendingMatchUpdates = [];
                postgres.saveMatches().then(() => {
                    extra2.map(match => {
                        updateMatchMetaTimestamp(match.Id);
                    });
                    extra2 = [];
                    scheduleMatchUpdate(match.Id, seconds(5));
                }).catch(() => {
                    logger.log('postgres', 'error', 'Unable to save match update, pushing to queue')
                });
            } else {
                logger.log('observer', 'info', 'Observer not finished syncing, delayed saving matches');
            }
        })
        .catch(err => {
            console.log(err)
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
    logger.log('observer', 'info', 'Attempting to seed matches')
    let numStr = 0;
    try {
        numStr = await broker.getNumMatches();
    } catch(e) {
        const err = `Failed to get number of matches when seeding`;
        logger.log('web3', 'error', err, {
            error: e
        });
        throw e;
    }
    const numMatches = parseInt(numStr);
    let promises = [...Array(numMatches)].map((_, idx) => {
        return broker.getFullMatchDetails(idx)
    });
    let matchSets = [];
    try {
        matchSets = await Promise.all(promises);
    } catch(e) {
        const err = 'Failed to get match information from contract';
        logger.log('web3', 'error', err, {
            error: e
        });
        throw e;
    }
    const matches = matchSets.map((matchData, idx) => {
        return broker.parseMatch(idx, matchData);
    });
    postgres.matches.push(...matches);
    try {
        await postgres.saveMatches();
    } catch(e) {
        const err = 'Failed to save matches in seed matches due to postgres errors';
        logger.log('postgres', 'error', err, {
            error: e
        });
        throw err;
    }
    try {
        await matches.map(match => seedRedisMatchKeysX(match.Id, match));
    } catch(e) {
        const err = 'Failed to seed redis keys for matches';
        logger.log('redis', 'error', err, {
            error: e
        });
        throw err;
    }
    matches.map(autoUpdateMatchUntilEnd);
}

const updateMatchCache = async function(matchId) {
    let matchData = null;
    try {
        matchData = await broker.getFullMatchDetails(matchId);
    } catch (err) {
        logger.log('web3', 'error', "Unable to get full match details - updateMatchCache", {
            error: err
        });
        throw "Unable to get full match details - updateMatchCache";
    }
    const match = broker.parseMatch(matchId, matchData);
    postgres.matches.push(match);
    try {
        await postgres.saveMatches();
    } catch (err) {
        logger.log('postgres', 'error', "Unable to save match details - updateMatchCache", {
            error: err
        });
        throw "Unable to save match details - updateMatchCache";
    }
    let resp = null;
    try {
        resp = await footballData.getFixtureAsync(match.FixtureId);
    } catch(err) {
        logger.log('http', 'error', "Unable to get football match data - updateMatchCache", {
            error: err
        });
        throw "Unable to get football match data - updateMatchCache";
    }
    if (resp.data.hasOwnProperty("error")) {
        logger.log('htttp', 'error', 'Got error when getting fixture');
        throw 'Got error when getting fixture';
    }
    let fixture = resp.data.fixture;
    const homeTeamScore = (fixture.result.goalsHomeTeam != null) ? fixture.result.goalsHomeTeam : 0;
    const awayTeamScore = (fixture.result.goalsAwayTeam != null) ? fixture.result.goalsAwayTeam : 0;
    let keys = getRedisMatchKeys(matchId);
    // [
    //     `${prefix}:status`,
    //     `${prefix}:score`,
    //     `${prefix}:totalNumBets`,
    //     `${prefix}:totalTeamA`,
    //     `${prefix}:totalTeamB`,
    //     `${prefix}:totalDraw`,
    //     `${prefix}:numPayoutAttempts`,
    //     `${prefix}:lastMatchMetaUpdate`,
    //     `${prefix}:lastMatchBetUpdate`
    // ];
    // TotalTeamABets: this.weiToEth(matchData[1][1]),
    //         TotalTeamBBets: this.weiToEth(matchData[1][2]),
    //         TotalDrawBets: this.weiToEth(matchData[1][3]),
            // NumBets: parseInt(matchData[1][4]),
    let actions = [
        ['set',  keys[0], fixture.status],
        ['set',  keys[1], `${homeTeamScore}-${awayTeamScore}`],
        ['set',  keys[2], match.NumBets],
        ['set',  keys[3], match.TotalTeamABets],
        ['set',  keys[4], match.TotalTeamBBets],
        ['set',  keys[5], match.TotalDrawBets],
        ['set',  keys[6], match.NumPayoutAttempts],
        ['set',  keys[7], Date.now()],
    ];
    try {
        // HERE MY NIGGA
        await redis.transaction(actions);
    } catch (err) {
        logger.log('redis', 'error', 'Unable to set redis match key - updateMatchCache', {
            error: err
        })
        throw 'Unable to set redis match key - updateMatchCache';
    }
    return match;
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
};

let timeouts = [];
const doLater = function(callBack, time) {
    timeouts.push(setTimeout(callBack, time));
};

const cancelAllLaters = function() {
    timeouts.map(global.clearTimeout);
};

// This function will update a match's info in postgres
// and in the redis cache including it's fixture
// starting 15 or less minutes before a match
const autoUpdateMatchUntilEnd = function(match) {
    let startTime = ((match.StartTime * 1000) - minutes(10) - Date.now());
    if (startTime < 0) {
        startTime = 0;
    }
    const update = async function(match) {
        logger.log('observer', 'info', `Gonna update cache for match ${match.Id}`)
        try {
            match = await updateMatchCache(match.Id);
        } catch (err)  {
            logger.log('observer', 'error', `Was not able to update match cache in autoupdater ${match.Id}`, {
                error: err
            });
        }
        // If it has not been 4 hours since the match started
        // or the match is not cancelled
        // or the winner is not set
        if (Date.now() < ((match.StartTime*1000) + hours(4)) && !match.Cancelled && match.Winner == 0) {
            doLater(() => update(match), minutes(5));
        }
    }
    // we want to run this at least once for all matches
    doLater(() => update(match), startTime);
};

let matchUpdateSchedule = new Map();
const scheduleMatchUpdate = function(matchId, time) {
    if (matchUpdateSchedule.has(matchId)) {
        return;
    }
    logger.log('observer', 'info', `Match update has been scheduled for match ${matchId} in ${time} milliseconds`)
    matchUpdateSchedule.set(matchId, true);
    doLater(function() {
        updateMatchCache(matchId);
        matchUpdateSchedule.delete(matchId);
    }, time);
}

const setPlacedBetRedisKeys = async function(bet, block, force = false) {
    logger.log('observer', 'info', `Setting bet paced event redis keys for bet ${bet.Match}:${bet.Id}`);
    let redisLastBetBlock = brokerConfig.StartBlockHeight;
    try {
        redisLastBetBlock = await redis.getKey(REDIS_LAST_BET_PLACED_BLOCK);
    } catch(e) {
        const err = `Failed to get redis key ${REDIS_LAST_BET_PLACED_BLOCK} in setPlacedBetRedisKeys`;
        logger.log('redis', 'error', err, {
            error: e
        });
        throw err;
    }
    if (block < parseInt(redisLastBetBlock) && !force) {
        return;
    }
    const matchKeys = getRedisMatchKeys(bet.Match);
    const userKeys = getRedisUserkeys(bet.Address.toLowerCase());
    await redis.transaction([
        ['set', REDIS_LAST_BET_PLACED_BLOCK, block],
        ['incr', REDIS_CONTRACT_NUM_BETS],
        ['incrbyfloat', REDIS_CONTRACT_TOTAL_BET, bet.Amount],
        ['incrbyfloat', userKeys[0], bet.Amount],
        ['sadd', userKeys[1], `${bet.Match}:${bet.Id}`],
        ['set', matchKeys[8], Date.now()],
    ]);
}

const setCancelledBetRedisKeys = async function(matchId, block, force = false) {
    logger.log('observer', 'info', `Setting bet cancelled event redis keys`);
    let redisLastCancelBlock = brokerConfig.StartBlockHeight;
    try {
        redisLastCancelBlock = await redis.getKey(REDIS_LAST_BET_CANCELLED_BLOCK);
    } catch(e) {
        const err = `Failed to get redis key ${REDIS_LAST_BET_CANCELLED_BLOCK} in setCanelledBetRedisKeys`;
        logger.log('redis', 'error', err, {
            error: e
        });
        throw err;
    }
    if (block < parseInt(redisLastCancelBlock) && !force) {
        return;
    }
    const matchKeys = getRedisMatchKeys(matchId);
    await redis.transaction([
        ['set', REDIS_LAST_BET_CANCELLED_BLOCK, block],
        ['set', matchKeys[8], Date.now()],
    ]);
}

const setClaimedBetRedisKeys = async function(matchId, block, force = false) {
    logger.log('observer', 'info', `Setting bet claimed event redis keys`);
    let redisLastClaimedBlock = brokerConfig.StartBlockHeight;
    try {
        redisLastClaimedBlock = await redis.getKey(REDIS_LAST_BET_CLAIMED_BLOCK);
    } catch(e) {
        const err = `Failed to get redis key ${REDIS_LAST_BET_CLAIMED_BLOCK} in setClaimedBetRedisKeys`;
        logger.log('redis', 'error', err, {
            error: e
        });
        throw err;
    }
    if (block < parseInt(redisLastClaimedBlock) && !force) {
        return;
    }
    const matchKeys = getRedisMatchKeys(matchId);
    await redis.transaction([
        ['set', REDIS_LAST_BET_CLAIMED_BLOCK, block],
        ['set', matchKeys[8], Date.now()],
    ]);
}

const betPlacedEventHandler = async function(err, result) {
    if (err == null) {
        logger.log('web3', 'info', `Attemptng to handle bet placed event in block ${result.blockNumber} from Tx ${result.transactionHash}`);
        const bet = broker.parseBetPlacedEvent(result);
        const eventBlock = result.blockNumber;
        pendingBetPlaces.push(bet);
        pendingLastBetPlacedBlock = eventBlock;
        if (finishedSyncing) {
            postgres.unpersistedBets.push(...pendingBetPlaces);
            pendingBetPlaces = [];
            scheduleMatchUpdate(bet.Match, seconds(3));
            postgres.saveBets()
            .then(() => { return setPlacedBetRedisKeys(bet, eventBlock, false) })
            .catch((err) => {
                logger.log('observer', 'error', 'Error occurred when attempting to save bets in bet placed event handler', {
                    error: err
                });
            })
        } else {
            logger.log('observer', 'info', 'Observer not finished syncing, delayed saving new bets');
        }
    } else {
        logger.log('web3', 'error', 'Error occured in web3 bet palced event handler', {
            error: err
        });
    }
}

const betCancelledEventHandler = async function(err, result) {
    if (err == null) {
        logger.log('web3', 'info', `Attemptng to handle bet cancelled event in block ${result.blockNumber} from Tx ${result.transactionHash}`)
        const bet = [parseInt(result.returnValues.matchId), parseInt(result.returnValues.betId)];
        const eventBlock = result.blockNumber;
        pendingBetCancels.push(bet);
        pendingLastBetCancelledBlock = eventBlock;
        if (finishedSyncing) {
            postgres.cancelledBets.push(...pendingBetCancels);
            pendingBetCancels = [];
            scheduleMatchUpdate(bet[0], seconds(5))
            postgres.saveBets()
            .then(() => { return setCancelledBetRedisKeys(bet[0], eventBlock) })
            .catch((err) => {
                logger.log('observer', 'error', 'Error occurred when attempting to save bets in bet cancelled event handler', {
                    error: err
                });
            })
        } else {
            logger.log('observer', 'info', 'Observer not finished syncing, delayed cancelling bets');
        }
    } else {
        logger.log('web3', 'error', 'Error occured in web3 bet cancelled event handler', {
            error: err
        });
    }
}

const betClaimedEventHandler = async function(err, result) {
    if (err == null) {
        (result);
        logger.log('web3', 'info', `Attemptng to handle bet claimed event in block ${result.blockNumber} from Tx ${result.transactionHash}`)
        const bet = [parseInt(result.returnValues.matchId), parseInt(result.returnValues.betId)];
        const eventBlock = result.blockNumber;
        pendingBetClaims.push(bet);
        pendingLastBetClaimedBlock = eventBlock;
        if (finishedSyncing) {
            postgres.claimedBets.push(...pendingBetClaims);
            pendingBetClaims = [];
            scheduleMatchUpdate(bet[0], seconds(5));
            postgres.saveBets()
            .then(() => { return setClaimedBetRedisKeys(bet[0], eventBlock) })
            .catch((err) => {
                logger.log('observer', 'error', 'Error occurred when attempting to save bets in bet claimed event handler', {
                    error: err
                });
            })
        } else {
            logger.log('observer', 'info', 'Observer not finished syncing, delayed updating claimed bets');
        }
    } else {
        logger.log('web3', 'error', 'Error occured in web3 bet claimed event handler', {
            error: err
        });
    }
}

const seedBets = async function() {
    logger.log('observer', 'info', 'Attempting to seed bets');
    let lastBetBlock = brokerConfig.StartBlockHeight;
    try {
        let temp = await redis.getKey(REDIS_LAST_BET_PLACED_BLOCK);
        lastBetBlock = parseInt(temp);
    } catch (e) {
        const err = `Unable to read ${REDIS_LAST_BET_PLACED_BLOCK} from redis`;
        logger.log('redis', 'error', err, {
            error: e
        });
        throw err;
    }

    let lastCancelBlock = brokerConfig.StartBlockHeight;
    try {
        let temp = await redis.getKey(REDIS_LAST_BET_CANCELLED_BLOCK);
        lastCancelBlock = parseInt(temp);
    } catch (e) {
        const err = `Unable to read ${REDIS_LAST_BET_CANCELLED_BLOCK} from redis`;
        logger.log('redis', 'error', err, {
            error: e
        });
        throw err;
    }

    let lastClaimBlock = brokerConfig.StartBlockHeight;
    try {
        let temp = await redis.getKey(REDIS_LAST_BET_CLAIMED_BLOCK);
        lastClaimBlock = parseInt(temp);
    } catch (e) {
        const err = `Unable to read ${REDIS_LAST_BET_CLAIMED_BLOCK} from redis`;
        logger.log('redis', 'error', err, {
            error: e
        });
        throw err;
    }
    
    let betEventLog = [];
    try {
        betEventLog = await broker.getBetPlacedEvents(lastBetBlock);
    } catch(e) {
        const err = `Error while trying to get past bet placed events`;
        logger.log('web3', 'error', err, {
            error: e
        });
        throw e;
    }

    let cancelEventLog = [];
    try {
        cancelEventLog = await broker.getBetCancelledEvents(lastCancelBlock);
    } catch(e) {
        const err = `Error while trying to get past bet cancelled events`;
        logger.log('web3', 'error', err, {
            error: e
        });
        throw e;
    }

    let claimEventLog = [];
    try {
        claimEventLog = await broker.getBetClaimedEvents(lastClaimBlock);
    } catch(e) {
        console.log(e)
        const err = `Error while trying to get past bet claimed events`;
        logger.log('web3', 'error', err, {
            error: e
        });
        throw e;
    }

    if (betEventLog.length > 0) {
        const betEventLogsLastBlock = betEventLog[betEventLog.length - 1].blockNumber;
        console.log(betEventLogsLastBlock)
        console.log(lastBetBlock)
        if ( betEventLogsLastBlock > lastBetBlock) {
            betEventLog = betEventLog.filter(event => event.blockNumber > lastBetBlock);
            let betsPlaced = betEventLog.map(event => broker.parseBetPlacedEvent(event));
            let blocks = betEventLog.map(event => event.blockNumber);
            postgres.unpersistedBets.push(...betsPlaced);
            try { 
                await postgres.saveBets();
                betsPlaced.map((bet,idx) => setPlacedBetRedisKeys(bet, blocks[idx], false)) ;
            } catch (e) {
                const err = 'Error occurred when attempting to seed bets placed bets';
                logger.log('observer', 'error', err, {
                    error: err,
                });
                throw e;
            }
        }
    }

    if (cancelEventLog.length > 0) {
        const cancelEventLogsLastBlock = cancelEventLog[cancelEventLog.length - 1].blockNumber;
        if ( cancelEventLogsLastBlock > lastCancelBlock) {
            cancelEventLog = cancelEventLog.filter(event => event.blockNumber > lastCancelBlock);
            let blocks = [];
            let betsCancelled = [];
            cancelEventLog.map(event => {
                betsCancelled.push([
                    parseInt(event.returnValues.matchId),
                    parseInt(event.returnValues.betId),
                ]);
                blocks.push(parseInt(event.blockNumber));
            });
            postgres.cancelledBets.push(...betsCancelled);
            try { 
                await postgres.saveBets();
                betsCancelled.map((bet, idx) => setCancelledBetRedisKeys(bet[0], blocks[idx]));
            } catch (e) {
                const err = 'Error occurred when attempting to seed cancelled bets';
                logger.log('observer', 'error', err, {
                    error: err,
                });
                throw e;
            }
        }
    }

    if (claimEventLog.length > 0) {
        const claimEventLogsLastBlock = claimEventLog[claimEventLog.length - 1].blockNumber;
        if ( claimEventLogsLastBlock > lastClaimBlock) {
            claimEventLog = claimEventLog.filter(event => event.blockNumber > lastClaimBlock);
            let blocks = [];
            let betsClaimed = [];
            claimEventLog.map(event => {
                betsClaimed.push([
                    parseInt(event.returnValues.matchId),
                    parseInt(event.returnValues.betId)
                ]);
                blocks.push(parseInt(event.blockNumber));
            });
            postgres.claimedBets.push(...betsClaimed);
            try { 
                await postgres.saveBets();
                betsClaimed.map((bet, idx) => setClaimedBetRedisKeys(bet[0], blocks[idx]));
            } catch (e) {
                const err = 'Error occurred when attempting to seed claimed bets';
                logger.log('observer', 'error', err, {
                    error: err,
                });
                throw e;
            }
        }
    }
};

const seedAll = async function() {
    try {
        cancelAllLaters();
        await seedMatches();
        await seedBets();
    } catch(e) {
        const err = "Attempt to seed matches and bets after websocket reconnect failed";
        logger.log('web3', 'error', err, {
            error: e
        });
    }
};

broker.addReconnectHandler(seedAll);

const finishedSyncingHandler = async function() {
    try {
        logger.log('observer', 'info', 'Finished syncing previous block history, attempting to save pending history');
        // We've seeded the teams, matches, and bets
        finishedSyncing = true;

        // save match events we found but didn't sync
        postgres.matches.push(...pendingMatchCreates);
        let extra = pendingMatchCreates;
        pendingMatchCreates = [];
        await postgres.saveMatches();
        await Promise.all(extra.map(match => {
            return Promise.all([
                seedRedisMatchKeysX(match.Id, match),
                autoUpdateMatchUntilEnd(match.Id)
            ]);
        }));
        extra = [];
        logger.log('observer', 'info', 'Saved pending match creates');

        postgres.matches.push(...pendingMatchUpdates);
        let extra2 = pendingMatchUpdates;
        pendingMatchUpdates = [];
        await postgres.saveMatches();
        await Promise.all(extra2.map(match => updateMatchMetaTimestamp(match.Id)));
        extra2 = [];
        logger.log('observer', 'info', 'Saved pending match updates');

        // save bet place events
        postgres.unpersistedBets.push(...pendingBetPlaces)
        await postgres.saveBets();
        await Promise.all(pendingBetPlaces.map(bet => setPlacedBetRedisKeys(bet, pendingLastBetPlacedBlock, true)));
        pendingBetPlaces = [];

        logger.log('observer', 'info', 'Saved pending placed bets');

        // save bet cancelled events
        postgres.cancelledBets.push(...pendingBetCancels);
        await postgres.saveBets();
        await redis.setKey(REDIS_LAST_BET_CANCELLED_BLOCK, pendingLastBetPlacedBlock);
        pendingBetCancels = [];
        logger.log('observer', 'info', 'Saved pending cancelled bets');

        postgres.claimedBets.push(...pendingBetClaims);
        await postgres.saveBets();
        await redis.setKey(REDIS_LAST_BET_CLAIMED_BLOCK, pendingLastBetClaimedBlock);
        pendingBetClaims = [];
    } catch (e) {
        const err = "Error occured in final syncing handler :/";
        logger.log('observer', 'error', err);
        throw err;
    }
};

const main = function() {
    const message = "Observer is now ...... well, observing I guess.";
    console.log(message);
    logger.log('observer', 'info', message);
};

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
    broker.addMatchUpdatedEventListener(matchEventHandler);
    broker.addMatchFailedPayoutReleaseEventListener(matchEventHandler);
})
.then(async () => {
    await seedMatches();
})
.then(() => {
    broker.addBetPlacedEventListener(betPlacedEventHandler);
    broker.addBetCancelledEventListener(betCancelledEventHandler);
    broker.addBetClaimedEventListener(betClaimedEventHandler);
})
.then(seedBets)
.then(finishedSyncingHandler)
.then(main)
.catch(err => {
    logger.log('observer', 'error', 'Unable to setup one of the essential observer services', {
        error: err
    });
    panic('observer', err);
});
