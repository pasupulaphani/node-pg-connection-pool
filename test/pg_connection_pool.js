require("should");
const Bluebird = require("bluebird");
const url = require("url");
const PgPool = require("../index");

describe("PgPool", () => {

  const options = {
    pgOptions: process.env.DATABASE_URL|| "postgres://postgres@localhost:5432/test_db"
  };

  describe("acquire", () => {

    it("should acquire connection with valid host", () => {

      const pool = new PgPool(options);

      return pool.acquire()
        .should.eventually.be.ok();
    });

    it("should wait to acquire if all used up", () => {
      const poolOptions = {
        min: 0,
        max: 1
      };
      const pool = new PgPool(Object.assign({}, options, {
        poolOptions: poolOptions
      }));

      pool.availableCount().should.be.equal(poolOptions.min);
      pool.getPoolSize().should.be.equal(poolOptions.min);
      pool.pendingCount().should.be.equal(0);

      return pool.acquire()
        .then(client => {
          pool.availableCount().should.be.equal(poolOptions.min);
          pool.getPoolSize().should.be.equal(1);
          pool.pendingCount().should.be.equal(0);
          return pool.release(client);
        })
        .then(() => pool.availableCount().should.be.equal(1))
        .then(() => pool.acquire())
        .then(() => {
          pool.availableCount().should.be.equal(0);
          pool.getPoolSize().should.be.equal(1);
          pool.pendingCount().should.be.equal(0);
        })
        .then(() => {
          pool.acquire(); // this is hanging op so no return
          return;
        })
        .then(() => {
          pool.availableCount().should.be.equal(0);
          pool.getPoolSize().should.be.equal(1);
          pool.pendingCount().should.be.equal(1);
        });
    });

    it("should not fail with many higher min connections", () => {
      const poolOptions = {
        min: 5,
        max: 10,
      };
      const pool = new PgPool(Object.assign({}, options, {
        poolOptions: poolOptions
      }));

      pool.acquire()
        .should.eventually.be.ok();
    });

    it("should invalid host fail acquire connection", () => {
      const pgOptions = Object.assign({}, options.pgOptions, {
        host: "UNAVAILABLE_HOST"
      });
      const pool = new PgPool(Object.assign({}, options, {
        pgOptions: pgOptions
      }));

      return pool.acquire().should.be.rejectedWith(Error, { message: "CONN_FAILED" });
    });

    it("should conn timeout fail acquire connection", () => {
      const poolOptions = {
        min : 1,
        acquireTimeoutMillis: 1
      };
      const pool = new PgPool(Object.assign({}, options, {
        poolOptions: poolOptions
      }));

      // make the conn is inuse
      pool.acquire()
        .then(conn => pool.release(conn));

      pool.acquire().should.be.rejectedWith(Error, { name: "TimeoutError" });
    });
  });

  describe("release", () => {

    const poolOptions = {
      min: 2,
      max: 4
    };
    const pool = new PgPool(Object.assign({}, options, {
      poolOptions: poolOptions
    }));

    it("should release connection with valid host", () => {

      pool.availableCount().should.be.equal(poolOptions.min);
      pool.getPoolSize().should.be.equal(poolOptions.min);
      pool.pendingCount().should.be.equal(0);

      return pool.acquire()
        .then(client => {
          pool.availableCount().should.be.equal(poolOptions.min - 1);
          return pool.release(client);
        })
        .then(() => pool.availableCount().should.be.equal(poolOptions.min));
    });

    it("should release connection with invalid host", () => {
      const pgOptions = Object.assign({}, options.pgOptions, {
        host: "UNAVAILABLE_HOST"
      });
      const pool = new PgPool(Object.assign({}, options, {
        pgOptions: pgOptions
      }));

      return pool.acquire()
        .catch(() => {
          return pool.release()
            .should.be.rejectedWith(/Resource not currently part of this pool/);
        });
    });
  });

  describe("destroy", () => {

    const poolOptions = {
      min: 2,
      max: 4
    };
    const pool = new PgPool(Object.assign({}, options, {
      poolOptions: poolOptions
    }));

    it("should destroy connection with valid host", () => {

      pool.availableCount().should.be.equal(poolOptions.min);
      pool.getPoolSize().should.be.equal(poolOptions.min);
      pool.pendingCount().should.be.equal(0);

      return pool.acquire()
        .then(client => {
          pool.availableCount().should.be.equal(poolOptions.min - 1);
          return pool.destroy(client);
        })
        .then(() => Bluebird.delay(100))
        .then(() => pool.availableCount().should.be.equal(poolOptions.min));
    });
  });

  describe("drain", () => {

    const poolOptions = {
      min: 2,
      max: 4
    };
    const pool = new PgPool(Object.assign({}, options, {
      poolOptions: poolOptions
    }));

    it("should drain all the coonections", () => {

      pool.availableCount().should.be.equal(poolOptions.min);
      pool.getPoolSize().should.be.equal(poolOptions.min);
      pool.pendingCount().should.be.equal(0);

      return pool.drain()
        .then(() => {
          pool.availableCount().should.be.equal(poolOptions.min);
          pool.getPoolSize().should.be.equal(poolOptions.min);
        });
      });
  });

  describe("getName", () => {

    it("should set given name", () => {

      const name = "testPool";
      const pool = new PgPool(Object.assign({}, options, {
        name: name
      }));

      pool.getName().should.be.equal(name);
    });

    it("should set random name if not set", () => {

      const pool = new PgPool(options);

      pool.getName().should.not.be.empty();
    });
  });

  describe("getPgOptions", () => {

    it("should set given postgres options", () => {

      const pool = new PgPool(options);

      const params = url.parse(options.pgOptions, true);
      const auth = params.auth.split(":");
      const expected = {
        user: auth[0],
        password: auth[1],
        host: params.hostname,
        port: params.port,
        database: params.pathname.split("/")[1],
        ssl: params.query.ssl || true
      };
      pool.getPgOptions().should.be.eql(expected);
    });
  });

  describe("getPoolOptions", () => {

    it("should set given pool options", () => {

      const poolOptions = {
        min: 2,
        max: 4
      };
      const pool = new PgPool(Object.assign({}, options, {
        poolOptions: poolOptions
      }));

      pool.getPoolOptions().should.be.equal(poolOptions);
    });
  });

  describe("status", () => {

    it("should get pool stats", () => {

      const name = "testPool";
      const poolOptions = {
        min: 2,
        max: 4
      };
      const pool = new PgPool(Object.assign({}, options, {
        name: name,
        poolOptions: poolOptions
      }));

      const status = pool.status();
      status.name.should.be.equal(name);
      status.size.should.be.equal(poolOptions.min);
      status.available.should.be.equal(0);
      status.pending.should.be.equal(0);
    });
  });

  describe("query", () => {

    const pool = new PgPool(options);

    it("should execute given query", () => {

      return pool.query("SELECT 1;")
        .then(res => res.rows[0]["?column?"])
        .should.eventually.be.eql(1);
    });

    it("should reject when cmd failed", () => {

      return pool.query("SEL;")
        .should.be.rejectedWith(/syntax error at or near/);
    });
  });
});
