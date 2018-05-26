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
        let provider = await this.setupWSProvider();
        this.wsWeb3 = new Web3(provider);
        this.httpWeb3 = new Web3(new Web3.providers.HttpProvider(this.httpEndpoint));
        this.wsBettingHouse = new this.wsWeb3.eth.Contract(this.abi, this.contractAddress);
        this.httpBettingHouse = new this.httpWeb3.eth.Contract(this.abi, this.contractAddress);
        this.backoff = 0;
    }

    setupWSProvider() {
        const handler = (resolve, reject) => {
            let provider = new Web3.providers.WebsocketProvider(this.wsEndpoint);
                provider.on('connect', (e) => {
                    this.backoff = 0;
                    this.numRejections = 0;
                    logger.log('web3', 'info', "Connected to broker websocket successfully!", {
                        instance: this.instance
                    });
                    resolve(provider);
                });
                const errorHandler = async (e) => {
                    this.backoff = this.numRejections * 2000;
                    this.numRejections++;
                    logger.log('web3', 'error', "Error has occured in Broker websocket provider, restarting provider!", {
                        error: e,
                        instance: this.instance,
                        numRejections: this.numRejections
                    });
                    if (this.numRejections >= 5) {
                        reject(e);
                    }
                    let provider = await this.setupWSProvider();
                    this.wsWeb3 = new Web3(provider);
                    await this.reconnectHandler();
                };
                const boundHandler = errorHandler.bind(this);
                provider.on('error', () => {this.removeListeners(); setTimeout(boundHandler, this.backoff)});
                provider.on('end', () => {this.removeListeners(); setTimeout(boundHandler, this.backoff)});
        };
        const boundHandler = handler.bind(this);
        return new Promise(boundHandler);
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

    getBalance() {
        return this.httpBettingHouse.getBalance().call();
    }
    
    addMatchCreatedEventListener(listener = function(error, result){}) {
        this.matchCreatedEventListener = listener;
        this.wsBettingHouse.events.MatchCreated(listener);
    }

    addMatchCancelledEventListener(listener = function(error, result){}) {
        this.matchCancelledEventListener = listener;
        this.wsBettingHouse.events.MatchCancelled(listener);
    }

    addMatchOverEventListener(listener = function(error, result){}) {
        this.matchOverEventListener = listener;
        this.wsBettingHouse.events.MatchOver(listener);
    }

    addMatchFailedAttemptedPayoutReleaseEventListener(listener = function(err, result){}) {
        this.matchFailedAttemptPayoutReleaseEventListener = listener;
        this.wsBettingHouse.events.MatchFailedAttemptedPayoutRelease(listener);
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

    resetListeners() {
        this.addMatchCreatedEventListener(this.matchCreatedEventListener);
        this.addMatchCancelledEventListener(this.matchCancelledEventListener);
        this.addMatchFailedAttemptedPayoutReleaseEventListener(this.matchFailedAttemptPayoutReleaseEventListener);
        this.addMatchFailedPayoutReleaseEventListener(this.matchFailedPayoutReleaseEvenetListener);
        this.addMatchOverEventListener(this.matchOverEventListener);
        this.addBetPlacedEventListener(this.betPlacedEventListener);
        this.addBetCancelledEventListener(this.betCancelledEventListener);
    }

    removeListeners() {
        const blank = function(){};
        this.wsBettingHouse.events.MatchCreated(blank);
        this.wsBettingHouse.events.MatchCancelled(blank);
        this.wsBettingHouse.events.MatchFailedAttemptedPayoutRelease(blank);
        this.wsBettingHouse.events.MatchFailedPayoutRelease(blank);
        this.wsBettingHouse.events.MatchOver(blank);
        this.wsBettingHouse.events.BetPlaced(blank);
        this.wsBettingHouse.events.BetCancelled(blank);
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
            HomeTeam: parseInt(matchData[0][2]),
            AwayTeam: parseInt(matchData[0][3]),
            Winner: parseInt(matchData[0][4]),
            StartTime: new Date(parseInt(matchData[0][5]) * 1000),
            CloseTime: new Date(parseInt(matchData[1][0]) * 1000),
            TotalTeamABets: this.weiToEth(matchData[1][1]),
            TotalTeamBBets: this.weiToEth(matchData[1][2]),
            TotalDrawBets: this.weiToEth(matchData[1][3]),
            NumBets: parseInt(matchData[1][4]),
            Cancelled: matchData[0][6],
            Locked: matchData[0][7],
            NumPayoutAttempts: parseInt(matchData[1][5]),
        };
    }

    parseBetPlacedEvent(betEvent) {
        return {
            Id: parseInt(betEvent.returnValues.betId),
            Address: betEvent.returnValues.better,
            Amount: this.weiToEth(betEvent.returnValues.amount),
            Decision: `${parseInt(betEvent.returnValues.outcome)}`,
            Match: parseInt(betEvent.returnValues.matchId),
            Cancelled: false,
        }
    }
};

module.exports.Broker = Broker;