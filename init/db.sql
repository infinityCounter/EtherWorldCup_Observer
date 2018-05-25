CREATE DATABASE ether_wc_2018;

\c ether_wc_2018

DROP TABLE IF EXISTS profiles;
CREATE TABLE IF NOT EXISTS profiles (
    better     varchar(42) PRIMARY KEY NOT NULL,
    amount_won numeric DEFAULT 0 NOT NULL
);

DROP TABLE IF EXISTS teams;
CREATE TABLE teams (
    id          int  PRIMARY KEY NOT NULL,
    external_id int  NOT NULL,
    rnk         int  NOT NULL,
    country     text NOT NULL,
    UNIQUE(country),
    UNIQUE(external_id)
);

DROP TYPE IF EXISTS MATCH_ROUND;
CREATE TYPE MATCH_ROUND AS ENUM (
    'GA', 'GB', 'GC', 'GD', 'GE', 'GF',
    'GG', 'GH', 'RO16', 'QF', 'SF', 'FL',
    'RP'
);

DROP TYPE IF EXISTS MATCH_RESULT;
CREATE TYPE MATCH_RESULT AS ENUM ('1','2','3');

DROP TABLE IF EXISTS matches;
CREATE TABLE matches (
    id                    int PRIMARY KEY NOT NULL,
    team_a                int REFERENCES teams NOT NULL,
    team_b                int REFERENCES teams NOT NULL,
    start_time            timestamp with time zone NOT NULL,
    betting_close_time    timestamp with time zone NOT NULL,
    mtch_round            MATCH_ROUND NOT NULL,
    winner                MATCH_RESULT NOT NULL,
    total_team_a_bets     numeric DEFAULT 0 NOT NULL, 
    total_team_b_bets     numeric DEFAULT 0 NOT NULL,
    total_draw_bets       numeric DEFAULT 0 NOT NULL,
    amount_won            numeric DEFAULT 0 NOT NULL,
    num_payouts_attempted int DEFAULT 0 NOT NULL,
    locked                boolean DEFAULT false NOT NULL,
    cancelled             boolean DEFAULT false NOT NULL
);

DROP TABLE IF EXISTS bets;
CREATE TABLE bets (
    id             int PRIMARY KEY NOT NULL,
    better         varchar(42) REFERENCES profiles NOT NULL,
    amount         numeric DEFAULT 0 NOT NULL,
    decision       MATCH_RESULT NOT NULL,
    match          int REFERENCES matches NOT NULL,
    cancelled      boolean DEFAULT false NOT NULL
);