const Web3             = require('web3');
const ganache          = require("ganache-cli");
const HDWalletProvider = require("truffle-hdwallet-provider");

const logger = require('../logging/logger.js');

let brokerInstance = 0;

class Broker {

    constructor(wsEndpoint, httpEndpoint, abi, contractAddr, startBlock, coinbase, mnemonic) {
      this.instance = brokerInstance++;
      this.abi = abi;
      this.contractAddress = contractAddr;
      this.startBlock = startBlock;
      this.wsEndpoint = wsEndpoint;
      this.httpEndpoint = httpEndpoint;
      this.coinbase = coinbase;
      this.numRejections = 0;
    }

    async setup() {
        let provider = await this.setupWSProvider();
        this.wsWeb3 = new Web3(provider);
        this.httpWeb3 = new Web3(new Web3.providers.HttpProvider(this.httpEndpoint));
        this.wsBettingHouse = new this.wsWeb3.eth.Contract(this.abi, this.contractAddress);
        this.httpBettingHouse = new this.httpWeb3.eth.Contract(this.abi, this.contractAddress);
    }

    setupWSProvider() {
        const handler = (resolve, reject) => {
            let provider = new Web3.providers.WebsocketProvider(this.wsEndpoint);
                provider.on('connect', (e) => {
                    this.numRejections = 0;
                    logger.log('web3', 'info', "Connected to broker websocket successfully!", {
                        instance: this.instance
                    });
                    resolve(provider);
                });
                const errorHandler = async (e) => {
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
                    this.resetListeners();
                    this.reconnectHandler();
                };
                const boundHandler = errorHandler.bind(this);
                provider.on('error', boundHandler);
                provider.on('end', boundHandler);
        };
        const boundHandler = handler.bind(this);
        return new Promise(boundHandler);
    }

    makeMatch(name, fixtureId, teamA, teamB, time) {
        return this.httpBettingHouse.methods.addMatch(name, fixtureId, teamA, teamB, time).send({
            from: this.coinbase,
        });
    }

    cancelMatch(matchId) {
        return this.httpBettingHouse.methods.cancelMatch(matchId).send({
          from: this.coinbase,
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

    changeMiniumBet(newMin) {
        return this.httpBettingHouse.changeMiniumBet(newMin).send({
            from: this.coinbase
        });
    }

    getCommissions() {
        return this.httpBettingHouse.methods.getCommissions().call();
    }

    withdrawCommissions() {
        return this.httpBettingHouse.methods.withdrawCommissions().send({
            from: this.coinbase
        });
    }

    getBalance() {
        return this.httpBettingHouse.getBalance().call();
    }
    
    withdrawBalance() {
        return this.httpBettingHouse.withdrawBalance.send({
            from: this.coinbase
        });
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

    getBetPlacedEvents(since = this.startBlock, callback = function(err, result){}) {
        return this.wsBettingHouse.getPastEvents('BetPlaced', { fromBlock: since }, callback);
    }

    addBetCancelledEventListener(listener = function(error, result){}) {
        this.betCancelledEventListener = listener;
        this.wsBettingHouse.events.BetCancelled(listener);
    }

    getBetCancelledEvents(since = this.startBlock, callback = function(err, result){}) {
        return this.wsBettingHouse.getPastEvents('BetCancelled', { fromBlock: since }, callback);
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
            Cancelled: matchData[0][6],
            Locked: matchData[0][7]
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