'use strict';

const AWS = require('aws-sdk');
const S3  = new AWS.S3({region : 'us-west-2'});

function env(name){
  if( ! (name in process.env) )
    throw new Error(`'${name}' not found in process.env`);

  return process.env[name];
}

function checkIfBucketExists(bucketName, callback) {
  S3.listBuckets(function(err, buckets){
    if(err) return callback(err);

    const bucketNames  = buckets.Buckets.map(b => b.Name);
    const bucketOIndex = bucketNames.indexOf(bucketName);
    if( bucketOIndex === -1)
      return callback();

    callback(null, buckets.Buckets[bucketOIndex]);
  });
}

function createBucketIfItDoesNotExist(bucketName, callback) {
  const region = _region || 'us-west-2';
  const s3_config = {
    Bucket: 'firehose-nodejs-example', 
    CreateBucketConfiguration: {
      LocationConstraint: 'us-west-2'
    }
  };

  checkIfBucketExists(bucketName, function(err, bucket){
    if(err) return callback(err);

    if(bucket) return callback(null, bucket);

    S3.createBucket(s3_config, function(err, bucket){
      if(err) return callback(err);

      callback(null, bucket);
    });
  })
}

module.exports = {
  checkIfBucketExists:          checkIfBucketExists,
  createBucketIfItDoesNotExist: createBucketIfItDoesNotExist,
};