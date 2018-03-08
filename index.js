const co = require('co');
const aws = require('aws-sdk');
const admin = require('firebase-admin');
const { getHttps } = require('s3-public-url');

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

// Return version id of a file from a s3 location.
const getVersionedUrl = co.wrap(function* getVersionedUrl(Bucket, Key) {
  const { VersionId } = yield s3.headObject({ Bucket, Key }).promise();
  const { LocationConstraint: region } = yield s3.getBucketLocation({ Bucket }).promise();

  return `${getHttps(Bucket, Key, region)}?versionid=${VersionId}`;
});

module.exports = {
  getDatabase,
  getVersionedUrl
};
