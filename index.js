'use strict';

// Loads custom ENV Variables
require('dotenv').load();

const myFirehose  = require('./my-firehose.js');
const myRedshift  = require('./my-redshift.js');
const myS3        = require('./my-s3.js');
const env         = require('./env.js');

const dStreamName = 'test_firehose_' + ~~(Math.random() * 10000);

  createS3Bucket               (
  createRedshiftTable          .bind(null,
  createDeliveryStream         .bind(null,
  waitForDStreamToBecomeActive .bind(null,
  sendOneRecordToFirehose      .bind(null,
  queryRedshiftTableEvery1min  )
  ))));


function createS3Bucket(callback){
  const bucketName = env('S3BUCKET_NAME');
  console.log('createS3Bucket', bucketName);

  myS3.createBucketIfItDoesNotExist(bucketName, function(err, res){
    if(err) throw new Error(err);

    callback(null, res);
  });
}

function createRedshiftTable(callback){
  console.log('createRedshiftTable');
  const sql_string = "\
  CREATE TABLE " + dStreamName + "(\
    id INT,\
    name VARCHAR(200),\
    created_at TIMESTAMP\
  );";
  console.log(sql_string);

  myRedshift.createTable(sql_string, function(err, res){
    if(err) throw new Error(err);

    callback(null, res);
  });
}

function createDeliveryStream(callback){
  console.log('createDeliveryStream');
  myFirehose.createDeliveryStream(dStreamName, function(err, res){
    if(err) throw new Error(err);

    callback(null, res);
  });
}

function waitForDStreamToBecomeActive(callback){
  console.log('waitForDStreamToBecomeActive');
  myFirehose.waitForDStreamToBecomeActive(dStreamName, function(err, res){
    if(err) throw new Error(err);

    callback(null, res);
  });
}

function sendOneRecordToFirehose(callback){
  console.log('sendOneRecordToFirehose');
  const record = {
    id:   1,
    name: "Daniel San",
    created_at: (new Date()).toISOString().substr(0, 19).replace('T',' '),
  };
  console.log(record);

  myFirehose.putRecord(dStreamName, record, function(err, res){
    if(err) throw new Error(err);

    callback(null, res);
  });
}

function queryRedshiftTableEvery1min(){
  console.log('\nqueryRedshiftTableEvery1min', new Date());

  myRedshift.selectAllFrom(dStreamName, function(err, res){
    if(err) throw new Error(err);

    console.log('rows', res.rows);
    setTimeout(queryRedshiftTableEvery1min, 60000, dStreamName);
  });
}