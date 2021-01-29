const mongodb = require('./mongodb');

const db = mongodb();

exports.handler = async (event, context) => {
  // see https://docs.atlas.mongodb.com/best-practices-connecting-to-aws-lambda/
  context.callbackWaitsForEmptyEventLoop = false;

  /**
   * Map all records to the appropriate tenant slug.
   */
  const opsMap = event.Records.reduce((map, record) => {
    const doc = JSON.parse(record.body);
    const { slug } = doc;
    if (!map.has(slug)) map.set(slug, []);

    const date = new Date(doc.ts);
    delete doc.ts;

    const filter = { _id: doc._id };
    const $setOnInsert = { ...doc, date };
    const op = { updateOne: { filter, update: { $setOnInsert }, upsert: true } };
    map.get(slug).push(op);
    return map;
  }, new Map());

  /**
   * Convert map into an array of bulk write operations per tenant.
   */
  await Promise.all(Array.from(opsMap, ([slug, ops]) => ({ slug, ops })).map(async ({ slug, ops }) => {
    const dbName = `p1-events-${slug}`;
    const collection = await db.collection({ dbName, name: 'events' });
    await collection.bulkWrite(ops);
  }));
};
