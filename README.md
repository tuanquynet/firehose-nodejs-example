# Kinesis Firehose Node.js example

## What does this example do
The idea of this example is to have a node.js code that uses [Amazon Kinesis Firehose](https://aws.amazon.com/kinesis/firehose/) to insert data do some [Amazon Redshift](https://aws.amazon.com/redshift/) table.

Because Firehose always sends data to a [S3](https://aws.amazon.com/s3/) bucket this example starts by creating one if it does not exist yet. The Firehose will be configured to send the data to a subfolder of your S3Bucket.

The steps followed by this example are the following: It...

1. Creates a S3Bucket (if it does not exist) with a specific name configured by you;
2. Connects directly to your Redshift Database and creates a table;
3. Creates a [Firehose Delivery Stream](http://docs.aws.amazon.com/firehose/latest/dev/basic-create.html)
4. Waits for the status of the Delivery Stream recently created to change from *Creating* to *Active*
5. Sends 1 record to the recently activated Firehose Delivery Stream
6. Connects directly to your Redshift Database and queries the table created on the step 2 once every 1 minute and prints out the retrieved rows.

## Installation
Simply clone the repo and install the modules using npm

    git clone https://github.com/danielsan/firehose-nodejs-example.git
    cd firehose-nodejs-example
    npm install

## Preparing
1. You need to have a valid account on Amazon Web Services
2. To run this example you will need to have your **aws_access_key_id** and your **aws_secret_access_key** within your **~/.aws/credentials** file.
3. You have to create your [Redshift Cluster](https://console.aws.amazon.com/redshift/home) manually
4. Once you have your cluster created connect to your Redshift Database using any valid client to create a user and password specifically for Firehose.
    * I to access your Redshift Database I use (and recommend) the [SQL Workbench](http://www.sql-workbench.net/downloads.html)
    * You can go to the [Cluster Connection URL](https://console.aws.amazon.com/redshift/home#cluster-connection:) to download the JDBC driver for Redshift and to see your JDBC Connection String as well.
    * Be sure to allow your ip address on redshift security settings
    * You'll also have to allow the Firehose servers of your region to access your Redshift Cluster.
6.
7. Copy the [.env-dist](.env-dist) file to a file named .env
8. Edit your .env file and set the variables with your AWS, S3 and Redshift information.

## Running
Once you have followed all the steps above you can run the project by following the npm pattern
executiong `npm start`

    npm start
