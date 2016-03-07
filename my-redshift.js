'use strict';

const pg = require('pg');
const redshift_conf = {
  host:     process.env.REDSHIFT_HOST,
  port:     process.env.REDSHIFT_PORT || 5439, // 5439 is the default redshift port
  database: process.env.REDSHIFT_DB,
  user:     process.env.REDSHIFT_USER,
  password: process.env.REDSHIFT_PASS,
};

function createTable(sql_string, callback) {
  pg.connect(redshift_conf, function pg_connect(err, client, done) {
    if(err) return callback(err);

    client.query(sql_string, function createTableCallback(err, result) {
      //call `done()` to release the client back to the pool
      done();

      // close the connection
      client.end();

      if(err) return callback(err);

      callback(null, result);
    });
  });
}

function selectAllFrom(table_name, callback) {
  const sql_string = "SELECT * FROM " + table_name + " LIMIT 10;";
  console.log({sql_string});

  pg.connect(redshift_conf, function pg_connect(err, client, done) {
    if(err) return callback(err);

    client.query(sql_string, function createTableCallback(err, result) {
      //call `done()` to release the client back to the pool
      done();

      // close the connection
      client.end();

      if(err) return callback(err);

      callback(null, result);
    });
  });

}

module.exports = {
  createTable:   createTable,
  selectAllFrom: selectAllFrom,
};