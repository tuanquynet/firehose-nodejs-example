'use strict';

const AWS      = require('aws-sdk');
const env      = require('./env.js');

const firehose = new AWS.Firehose({
  apiVersion: '2015-08-04',
  region : env('AWS_REGION'),
  accessKeyId: env('AWS_ACCESS_KEY_ID'),
  secretAccessKey: env('AWS_SECRET_ACCESS_KEY'),
});

// AWS.config.logger = process.stdout;
AWS.config.logger = console;

function createDeliveryStream(dStreamName) {

  const REDSHIFT_JDBCURL='jdbc:redshift://' + env('REDSHIFT_HOST') + ':' + env('REDSHIFT_PORT') + '/' + env('REDSHIFT_DB');
  const
    s3config = { /* required */
      BucketARN: 'arn:aws:s3:::' + env('S3BUCKET_NAME'), /* required */
      RoleARN: `arn:aws:iam::${env('AWS_ACCOUNT_ID')}:role/rnd_firehose_delivery`, /* required */
      BufferingHints: {
        IntervalInSeconds: 60,
        SizeInMBs: 128
      },
      // CompressionFormat: 'UNCOMPRESSED | GZIP | ZIP | Snappy',
      CompressionFormat: 'UNCOMPRESSED',
      EncryptionConfiguration: {
        // KMSEncryptionConfig: {
        //   AWSKMSKeyARN: 'STRING_VALUE' /* required */
        // },
        NoEncryptionConfig: 'NoEncryption'
      },
      Prefix: dStreamName + '/',
    },

    redshift_config = {
      DeliveryStreamName: dStreamName, /* required */
      RedshiftDestinationConfiguration: {
        ClusterJDBCURL: REDSHIFT_JDBCURL, /* required */
        CopyCommand: { /* required */
          DataTableName:    dStreamName, /* required */
          CopyOptions:      "FORMAT AS json 'auto'",
          // DataTableColumns: 'STRING_VALUE'
        },
        Username: env('REDSHIFT_USER'), /* required */
        Password: env('REDSHIFT_PASS'), /* required */

        // RoleARN: 'STRING_VALUE', /* required */
        // http://docs.aws.amazon.com/general/latest/gr/aws-arns-and-namespaces.html#arn-syntax-kinesis-firehose
        // arn:aws:firehose:region:account-id:deliverystream/delivery-stream-name
        // RoleARN: `arn:aws:firehose:${env('AWS_REGION')}:${env('AWS_ACCOUNT_ID')}:cluster:${env('REDSHIFT_CLUSTER_NAME')}`, /* required */
        // RoleARN: `arn:aws:iam::${env('AWS_ACCOUNT_ID')}:role:firehose_delivery_role`, /* required */
        RoleARN: s3config.RoleARN,
        S3Configuration: s3config,
      },
      // S3DestinationConfiguration: s3config
    };

  // console.log('redshift_config', JSON.stringify(redshift_config, null, ' '));
  console.log('s3config', JSON.stringify(s3config, null, ' '));
  // Create the new stream if it does not already exist.
  return firehose.createDeliveryStream(s3config).promise();
}

async function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
};

async function waitForDStreamToBecomeActive(dStreamName) {

  const result = firehose.describeDeliveryStream({DeliveryStreamName : dStreamName});
  
  if (result.DeliveryStreamDescription.DeliveryStreamStatus === 'ACTIVE'){
    console.log(dStreamName, 'is now active');
    return result;
  }

  // The stream is not ACTIVE yet. Wait for another 5 seconds before
  // checking the state again.
  process.stdout.write('.');
  
  await sleep(5000);

  return await waitForDStreamToBecomeActive(dStreamName);
}

// http://docs.aws.amazon.com/AWSJavaScriptSDK/latest/AWS/Firehose.html#putRecord-property
function putRecord(dStreamName, data) {
  var recordParams = {
    Record: {
      Data: JSON.stringify(data)
    },
    DeliveryStreamName: dStreamName
  };
 
  return firehose.putRecord(recordParams).promise();
}

function listDeliveryStreams() {
  return firehose.listDeliveryStreams({
    DeliveryStreamType: 'DirectPut',
    ExclusiveStartDeliveryStreamName: 'ListDeliveryStreams',
    Limit: 10,
  }).promise();
}

module.exports = {
  createDeliveryStream,
  waitForDStreamToBecomeActive,
  putRecord,
  listDeliveryStreams,
};