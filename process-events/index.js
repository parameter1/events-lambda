const mongodb = require('./mongodb');

const db = mongodb();

exports.handler = async (event, context) => {
  // see https://docs.atlas.mongodb.com/best-practices-connecting-to-aws-lambda/
  context.callbackWaitsForEmptyEventLoop = false;

  // @todo how to handle for multiple tenant slugs?
  const collection = await db.collection({ dbName: 'p1-events-acbm-fcp', name: 'events' });
  const ops = event.Records.map((record) => {
    const doc = JSON.parse(record.body);
    const date = new Date(doc.ts);
    delete doc.ts;

    const filter = { _id: doc._id };
    const $setOnInsert = {
      ...doc,
      date,
    };
    return { updateOne: { filter, update: { $setOnInsert }, upsert: true } };
  });
  if (ops.length) await collection.bulkWrite(ops);
};
