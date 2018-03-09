const co = require('co');
const aws = require('aws-sdk');
const admin = require('firebase-admin');
const { getHttps } = require('s3-public-url');
const { backup, restore } = require('./backupRestoreDb');

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

// Get log function.
function getLog(database) {
  return function log(message, type = 'info') {
    console[type](message);

    database.collection('logs').add({
      message,
      type,
      timestamp: admin.firestore.FieldValue.serverTimestamp()
    });
  };
}

// Return version id of a file from a s3 location.
const getVersionedUrl = co.wrap(function* getVersionedUrl(Bucket, Key) {
  const { VersionId } = yield s3.headObject({ Bucket, Key }).promise();
  const { LocationConstraint: region } = yield s3.getBucketLocation({ Bucket }).promise();

  return `${getHttps(Bucket, Key, region)}?versionid=${VersionId}`;
});

// Create a snapshot for an episode.
const createEpisodeSnapshot = co.wrap(function* createEpisodeSnapshot(bucket, episodeId, version) {
  const database = yield getDatabase(bucket);

  const results = (yield Promise.all([
    backup(yield database.doc(`episodes/${episodeId}`).get()),
    backup(yield database.collection('cues').where('episodeId', '==', episodeId).get())
  ])).reduce((acc, it) => acc.concat(it), []);

  const ref = yield database.collection('snapshots').add({
    type: 'episode',
    id: episodeId,
    version,
    timestamp: admin.firestore.FieldValue.serverTimestamp()
  });

  restore(database.doc(`snapshots/${ref.id}`), results);
});

// Restore a snapshot for an episode.
const restoreEpisodeSnapshot = co.wrap(function* restoreEpisodeSnapshot(bucket, episodeId, version) {
  const database = yield getDatabase(bucket);

  const querySnapshot = yield database
    .collection('snapshots')
    .where('type', '==', 'episode')
    .where('id', '==', episodeId)
    .where('version', '==', version)
    .limit(1)
    .get();

  const { id } = querySnapshot.docs[0];

  const results = (yield Promise.all([
    backup(yield database.doc(`snapshots/${id}/episodes/${episodeId}`).get()),
    backup(yield database.collection(`snapshots/${id}/cues`).get())
  ]))
    .reduce((acc, it) => acc.concat(it), [])
    .map(({ data, path }) => ({
      data,
      path: path.split('/').splice(2).join('/')
    }));

  restore(database, results);
});

module.exports = {
  getDatabase,
  getLog,
  getVersionedUrl,
  createEpisodeSnapshot,
  restoreEpisodeSnapshot
};
