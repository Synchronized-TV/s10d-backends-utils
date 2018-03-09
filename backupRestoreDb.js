const co = require('co');
const {
  constructFirestoreDocumentObject,
  constructDocumentObjectToBackup,
  saveDocument
} = require('firestore-backup-restore/build/lib/FirestoreDocument');

const backupDocument = co.wrap(function* backupDocument(document) {
  const collections = yield document.ref.getCollections();

  let results = [{
    data: constructDocumentObjectToBackup(document.data()),
    path: document.ref.path
  }];

  for (const collection of collections) {
    const snapshot = yield collection.get();
    const childrenResults = yield backupCollection(snapshot);
    results = results.concat(childrenResults);
  }

  return results;
});

const backupCollection = co.wrap(function* backupCollection(snapshot) {
  const documents = [];
  snapshot.forEach(document => documents.push(document));

  let results = [];

  for (const document of documents) {
    const childrenResults = yield backupDocument(document);
    results = results.concat(childrenResults);
  }

  return results;
});

const backup = co.wrap(function* backup(entry) {
  const results = entry.ref
    ? backupDocument(entry)
    : backupCollection(entry);

  return results;
});

const restore = co.wrap(function* restore(firestore, results) {
  for (const { data, path } of results) {
    const documentId = path.split('/').pop();
    const pathWithoutId = path.substr(0, path.lastIndexOf('/'));
    const documentData = constructFirestoreDocumentObject(data, { firestore });

    yield saveDocument(
      firestore,
      pathWithoutId,
      documentId,
      documentData
    );
  }

  console.info('db:restore finished restore');
});

module.exports = {
  backup,
  restore
};
