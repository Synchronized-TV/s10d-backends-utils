const co = require('co');
const aws = require('aws-sdk');
const admin = require('firebase-admin');

const s3 = new aws.S3();

// Get db from credentials stored on s3.
const getDatabase = co.wrap(function* getDatabase(bucket) {
  const credentialsObject = yield s3.getObject({ Bucket: bucket, Key: 'firebase-adminsdk.json' }).promise();
  const credentials = JSON.parse(credentialsObject.Body.toString());
  const database = !admin.apps.length
    ? admin.initializeApp({ credential: admin.credential.cert(credentials) }).firestore()
    : admin.app().firestore();

  return database;
});

module.exports = {
  getDatabase
};
