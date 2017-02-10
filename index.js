"use strict";

const genericPool = require("generic-pool");
const retry = require("retry-as-promised");
const pick = require("lodash.pick");
const pg = require("pg");

const debug = require("debug")("simplePgPool");

function injectLogger (logger) {
 if (!logger) {
   logger = {};
 }

 return Object.assign({
   log: function () {},
   debug: function () {},
   info: function () {},
   warn: function () {},
   error: function () {}
 }, logger);
}

const create = function (pgOptions) {
  return new Promise((resolve, reject) => {

    let client;
    if (pgOptions.native === true && !pg.hasOwnProperty("native")) {

      const err = new Error("pg-native is installed");
      err.message = "Native bindings not available. Make sure pg-native is installed.";
      throw err;

    } else if (pgOptions.native === true) {

      client = new pg.native.Client(pgOptions);

    } else {

      client = new pg.Client(pgOptions);
    }

    client.on("error", function (err) {
      reject(err);
    });

    client.connect(function (err) {
      if (err) {
        reject(err);
      } else {
        resolve(client);
      }
    });
  });
};


/**
 * @constructor
 * @param    {object}   options - Accepts properties ["name", "pgOptions", "poolOptions", "logger"]
 * @param    {string}   options.name - Name your pool
 * @param    {object}   options.pgOptions - opts from [node-postgres/wiki/Client#parameters]{@link https://github.com/brianc/node-postgres/wiki/Client#parameters}
 * @param    {object}   options.poolOptions - opts from [node-pool#createpool]{@link https://github.com/coopernurse/node-pool#createpool}
 * @param    {object}   options.logger - Inject your custom logger
 */
const PgPool = module.exports = function (options) {

  options = pick(options, ["name", "pgOptions", "poolOptions", "logger"]);

  this.name = options.name || `pgPool-${Math.random().toString(36).substr(2, 10)}`;
  this.pgOptions = options.pgOptions;
  this.poolOptions = options.poolOptions;
  this.logger = injectLogger(options.logger);

  const factory = {
    create: () => {

      // for retry
      let createAttempts = 0;

      // this is due to the limitation of node-pool ATM
      // https://github.com/coopernurse/node-pool/issues/161#issuecomment-261109882
      return retry(function () {
        createAttempts++;
        if (createAttempts > 3) {
          const err = new Error("CONN_FAILED");
          debug("Max conn createAttempts reached: %s, resolving to error:", createAttempts, err);

          // reset for next try
          createAttempts = 0;
          return Promise.resolve(err);
        }

        return create(options.pgOptions);
      }, {
        max: 10,
        name: "factory.create",
        report: debug
      });
    },
    destroy: (client) => {
      return new Promise((resolve) => {

        try {
          // Flush when closing.
          client.end(true, () => resolve());
          debug("Client conn closed. Available count : %s. Pool size: %s", this.availableCount(), this.getPoolSize());
          this.logger.log("Client conn closed. Available count : %s. Pool size: %s", this.availableCount(), this.getPoolSize());

        } catch (err) {
          debug("Failed to destroy connection", err);
          this.logger.error("Failed to destroy connection", err);

          // throw error cause infinite event loop; limitation of node-pool
          // throw err;
        }
      });
    }
  };

  // Now that the pool settings are ready create a pool instance.
  debug("Creating pool", this.poolOptions);
  this.pool = genericPool.createPool(factory, this.poolOptions);

  this.pool.on("factoryCreateError", e => {
    debug("Errored while connecting Postgres", e);
    this.logger.error("Errored while connecting Postgres", e);
  });
  this.pool.on("factoryDestroyError", e => {
    debug("Errored while destroying Postgres conn", e);
    this.logger.error("Errored while destroying Postgres conn", e);
  });
};

/**
 * Send Postgres instructions
 *
 * @param {string} commandName - Name of the command
 * @param {array}  commandArgs - Args sent to the command
 * @returns {promise} Promise resolve with the result or Error
 */
PgPool.prototype.query = function (queryString) {

  return this.pool.acquire()
    .then(client => {

      return client.query(queryString)
        .then(result => {

          this.pool.release(client);
          return result;
        })
        .catch(err => {
          this.pool.release(client);
          this.logger.error("Errored query", err);
          debug("Errored query", err);
          throw err;
        });
    });
};

/**
 * Acquire a Postgres connection and use an optional priority.
 *
 * @param {number} priority - priority list number
 * @param {number} db - Use the db with range {0-16}
 * @returns {promise} Promise resolve with the connection or Error
 */
PgPool.prototype.acquire = function (priority) {
  return this.pool.acquire(priority)
    .then(client => {

      if (client instanceof Error) {
        debug("Couldn't acquire connection to %j", this.pgOptions);
        this.logger.error("Couldn't acquire connection to %j", this.pgOptions);
        throw client;
      }
      return client;
    });
};

/**
 * Release a Postgres connection to the pool.
 *
 * @param {object} client - Postgres connection
 * @returns {promise} Promise
 */
PgPool.prototype.release = function (client) {
  return this.pool.release(client);
};

/**
 * Destroy a Postgres connection.
 *
 * @param {object} client - Postgres connection
 * @returns {promise} Promise
 */
PgPool.prototype.destroy = function (client) {
  return this.pool.destroy(client);
};

/**
 * Drains the connection pool and call the callback id provided.
 *
 * @returns {promise} Promise
 */
PgPool.prototype.drain = function () {
  return this.pool.drain(() => this.pool.clear());
};

/**
 * Returns factory.name for this pool
 *
 * @returns {string} Name of the pool
 */
PgPool.prototype.getName = function () {
  return this.name;
};

/**
 * Returns this.pgOptions for this pool
 *
 * @returns {object} Postgres options given
 */
PgPool.prototype.getPgOptions = function () {
  return this.pgOptions;
};

/**
 * Returns this.poolOptions for this pool
 *
 * @returns {object} pool options given
 */
PgPool.prototype.getPoolOptions = function () {
  return this.poolOptions;
};

/**
 * Returns size of the pool
 *
 * @returns {number} size of the pool
 */
PgPool.prototype.getPoolSize = function () {
  return this.pool.size;
};

/**
 * Returns available connections count of the pool
 *
 * @returns {number} available connections count of the pool
 */
PgPool.prototype.availableCount = function () {
  return this.pool.available;
};

/**
 * Returns pending connections count of the pool
 *
 * @returns {number} pending connections count of the pool
 */
PgPool.prototype.pendingCount = function () {
  return this.pool.pending;
};

/**
 * Returns pool status and stats
 *
 * @returns {object} pool status and stats
 */
PgPool.prototype.status = function () {
  return {
    name: this.name,
    size: this.pool.size,
    available: this.pool.available,
    pending: this.pool.pending
  };
};
