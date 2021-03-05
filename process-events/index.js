const AWS = require('aws-sdk'); // eslint-disable-line import/no-extraneous-dependencies
const { nanoid } = require('nanoid');
const mongodb = require('./mongodb');

const db = mongodb();
const sqs = new AWS.SQS();

const { ARCHIVE_QUEUE_URL } = process.env;

exports.handler = async (event, context) => {
  // see https://docs.atlas.mongodb.com/best-practices-connecting-to-aws-lambda/
  context.callbackWaitsForEmptyEventLoop = false;

  const toArchiveEntries = [];

  /**
   * Map all records to the appropriate tenant slug.
   */
  const opsMap = event.Records.reduce((map, record) => {
    const doc = JSON.parse(record.body);

    // push data to the archive batch.
    toArchiveEntries.push({
      Id: nanoid(10),
      MessageBody: record.body,
      MessageDeduplicationId: doc._id,
      MessageGroupId: doc.slug,
    });

    const { slug } = doc;
    if (!map.has(slug)) map.set(slug, []);

    // replace the timestamp with a Date
    const date = new Date(doc.ts);
    delete doc.ts;

    // delete the slug. no need to save it once we've determined the DB
    delete doc.slug;

    const filter = { _id: doc._id };
    const $setOnInsert = { ...doc, date };
    const op = { updateOne: { filter, update: { $setOnInsert }, upsert: true } };
    map.get(slug).push(op);
    return map;
  }, new Map());

  /**
   * Convert map into an array of bulk write operations per tenant.
   */
  await Promise.all(Array.from(opsMap, ([slug, ops]) => ({ slug, ops }))
    .map(async ({ slug, ops }) => {
      const dbName = `p1-events-${slug}`;
      const collection = await db.collection({ dbName, name: 'events' });
      await collection.bulkWrite(ops);
    }));

  await sqs.sendMessageBatch({ QueueUrl: ARCHIVE_QUEUE_URL, Entries: toArchiveEntries }).promise();
};
