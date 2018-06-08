const Web3             = require('web3');

const logger = require('../logging/logger.js');

let brokerInstance = 0;

class Broker {

    constructor(wsEndpoint, httpEndpoint, abi, contractAddr, startBlock, coinbase) {
      this.instance = brokerInstance++;
      this.abi = abi;
      this.contractAddress = contractAddr;
      this.startBlock = startBlock;
      this.wsEndpoint = wsEndpoint;
      this.httpEndpoint = httpEndpoint;
      this.numRejections = 0;
    }

    async setup() {
        await this.setupWS();
        this.httpWeb3 = new Web3(new Web3.providers.HttpProvider(this.httpEndpoint));
        this.httpBettingHouse = new this.httpWeb3.eth.Contract(this.abi, this.contractAddress);
        this.backoff = 0;
    }

    setupWS() {
        const handler = (resolve, reject) => {
            let provider = new Web3.providers.WebsocketProvider(this.wsEndpoint);
            
            provider.on('connect', (e) => {
                this.wsBettingHouse = new this.wsWeb3.eth.Contract(this.abi, this.contractAddress);
                this.backoff = 0;
                this.numRejections = 0;
                logger.log('web3', 'info', "Connected to broker websocket successfully!", {
                    instance: this.instance
                });
                resolve();
            });

            const errorHandler = async () => {
                this.backoff = this.numRejections * 1200;
                this.numRejections++;
                logger.log('web3', 'error', "Restarting ws provider after error!", {
                    instance: this.instance,
                    numRejections: this.numRejections
                });
                if (this.numRejections >= 5) {
                    reject(e);
                }
                this.wsWeb3 = await this.setupWS();
                await this.reconnectHandler();
            };

            const boundHandler = errorHandler.bind(this);
            provider.on('error', (e) => {
                this.disconnect(e);
                setTimeout(boundHandler, this.backoff);
            });
            provider.on('end', (e) => {
                this.disconnect(e);
                setTimeout(boundHandler, this.backoff);
            });

            this.wsWeb3 = new Web3(provider);
            this.wsWeb3.eth.subscribe('newBlockHeaders', (error, blockHeader) => {
                if (error) return console.error(error);;
            }).on('data', (_) => {});
        };
        const boundHandler = handler.bind(this);
        return new Promise(boundHandler);
    }
    
    disconnect(e) {
        logger.log('web3', 'error', "Error has occured in Broker websocket provider, disconnecting now!", {
            instance: this.instance
        });
    }

    getBet(matchId, betId) {
        return this.httpBettingHouse.methods.getBet(matchId, betId).call();
    }

    getNumMatches() {
        return this.httpBettingHouse.methods.getNumMatches().call();
    }

    getMatch(matchId) {
        return this.httpBettingHouse.methods.getMatch(matchId).call();
    }

    getMatchBettingDetails(matchId) {
        return this.httpBettingHouse.methods.getMatchBettingDetails(matchId).call();
    }

    getFullMatchDetails(matchId) {
        return Promise.all([
            this.getMatch(matchId),
            this.getMatchBettingDetails(matchId)
        ]);
    }

    getTeam(teamId) {
        return this.httpBettingHouse.methods.TEAMS(teamId).call();
    }
    
    getMinimumBet() {
        return this.httpBettingHouse.methods.getMinimumBet().call();
    }

    getCommissions() {
        return this.httpBettingHouse.methods.getCommissions().call();
    }

    addMatchCreatedEventListener(listener = function(error, result){}) {
        this.matchCreatedEventListener = listener;
        this.wsBettingHouse.events.MatchCreated(listener);
    }

    addMatchUpdatedEventListener(listener = function(error, result){}) {
        this.matchUpdatedEventListener = listener;
        this.wsBettingHouse.events.MatchUpdated(listener);
    }

    addMatchFailedPayoutReleaseEventListener(listener = function(error, result){}) {
        this.matchFailedPayoutReleaseEvenetListener = listener;
        this.wsBettingHouse.events.MatchFailedPayoutRelease(listener);
    }

    addBetPlacedEventListener(listener = function(error, result){}) {
        this.betPlacedEventListener = listener;
        this.wsBettingHouse.events.BetPlaced(listener);
    }

    getBetPlacedEvents(since = this.startBlock) {
        let that = this;
        return new Promise((resolve, request) => {
            that.wsBettingHouse.getPastEvents('BetPlaced', {fromBlock: since, to: "latest"}, (err, result) => {
                if (err != null) {
                    reject(err);
                    return;
                } else {
                    resolve(result);
                }
            });
        });
    }

    addBetCancelledEventListener(listener = function(error, result){}) {
        this.betCancelledEventListener = listener;
        this.wsBettingHouse.events.BetCancelled(listener);
    }

    getBetCancelledEvents(since = this.startBlock) {
        let that = this;
        return new Promise((resolve, request) => {
            that.wsBettingHouse.getPastEvents('BetCancelled', {fromBlock: since, to: "latest"}, (err, result) => {
                if (err != null) {
                    reject(err);
                    return;
                } else {
                    resolve(result);
                }
            });
        });
    }

    addBetClaimedEventListener(listener = function(error, result){}) {
        this.betClaimedEventListener = listener;
        this.wsBettingHouse.events.BetClaimed(listener);
    }

    getBetClaimedEvents(since = this.startBlock) {
        let that = this;
        return new Promise((resolve, request) => {
            that.wsBettingHouse.getPastEvents('BetClaimed', {fromBlock: since, to: "latest"}, (err, result) => {
                if (err != null) {
                    reject(err);
                    return;
                } else {
                    resolve(result);
                }
            });
        });
    }

    resetListeners() {
        this.addMatchCreatedEventListener(this.matchCreatedEventListener);
        this.addMatchUpdatedEventListener(this.matchUpdatedEventListener);
        this.addMatchFailedPayoutReleaseEventListener(this.matchFailedPayoutReleaseEvenetListener);
        this.addBetPlacedEventListener(this.betPlacedEventListener);
        this.addBetCancelledEventListener(this.betCancelledEventListener);
        this.addBetClaimedEventListener(this.betClaimedEventListener);
    }

    addReconnectHandler(handler = function(){}) {
        this.reconnectHandler = handler;
    }

    weiToEth(amount) {
        return this.httpWeb3.utils.fromWei(amount, 'ether');
    }

    parseMatch(matchId, matchData) {
        return {
            Id: matchId,
            Name: matchData[0][0],
            FixtureId: parseInt(matchData[0][1]),
            SecondaryFixtureId: parseInt(matchData[0][2]),
            Inverted: matchData[0][3],
            HomeTeam: parseInt(matchData[0][4]),
            AwayTeam: parseInt(matchData[0][5]),
            Winner: parseInt(matchData[0][6]),
            StartTime: parseInt(matchData[0][7]),
            CloseTime: parseInt(matchData[1][0]),
            TotalTeamABets: this.weiToEth(matchData[1][1]),
            TotalTeamBBets: this.weiToEth(matchData[1][2]),
            TotalDrawBets: this.weiToEth(matchData[1][3]),
            NumBets: parseInt(matchData[1][4]),
            Cancelled: matchData[0][8],
            Locked: matchData[0][9],
            NumPayoutAttempts: parseInt(matchData[1][5]),
        };
    }

    parseBetPlacedEvent(betEvent) {
        return {
            Id: parseInt(betEvent.returnValues.betId),
            Address: betEvent.returnValues.better.toLowerCase(),
            Amount: this.weiToEth(betEvent.returnValues.amount),
            Decision: `${parseInt(betEvent.returnValues.outcome)}`,
            Match: parseInt(betEvent.returnValues.matchId),
            Cancelled: false,
            Claimed: false,
            Block: parseInt(betEvent.blockNumber),
        }
    }
};

module.exports.Broker = Broker;