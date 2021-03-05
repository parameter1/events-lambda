const objectHash = require('object-hash');
const timezone = require('dayjs/plugin/timezone');
const utc = require('dayjs/plugin/utc');
const dayjs = require('dayjs').extend(utc).extend(timezone);
const mongodb = require('./mongodb');

const keys = [
  'realm',
  'host',
  'env',
  'act',
  'cat',
  'lab',
  'ent',
  'ctx',
  'props',
];

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

    // replace the timestamp with a Date
    const date = new Date(doc.ts);
    // calculate the month.
    const month = dayjs(date).tz('UTC').startOf('month').toDate();

    // create the formatted event that will be saved
    const formatted = keys.reduce((o, key) => {
      let value = key === 'props' ? (doc[key] || {}) : (doc[key] || '').trim();
      if (key === 'props' && value._id && Object.keys(value).length === 1) {
        // the only prop key that exists is the `_id`. unset.
        value = {};
      }
      return { ...o, [key]: value };
    }, {});

    // create the hash from the formatted object
    // this will become the unique event id
    const hash = objectHash(formatted);

    // the upsert criteria
    const filter = { month, 'event.hash': hash };
    // the update settings
    const update = {
      $setOnInsert: {
        month,
        event: { ...formatted, hash },
      },
      $inc: { count: 1 }, // increment the event count
      $addToSet: {
        // if there's an identity, push the `idt` to the identities array
        // if there isn't an identity, push the `vis` to the visitors array
        ...(doc.idt && { identities: doc.idt }),
        ...(!doc.idt && { visitors: doc.vis }),
      },
      // add first and last seen at dates
      $min: { firstSeenAt: date },
      $max: { lastSeenAt: date },
    };
    const op = { updateOne: { filter, update, upsert: true } };
    map.get(slug).push(op);
    return map;
  }, new Map());

  /**
   * Convert map into an array of bulk write operations per tenant.
   */
  await Promise.all(Array.from(opsMap, ([slug, ops]) => ({ slug, ops }))
    .map(async ({ slug, ops }) => {
      const dbName = `p1-events-${slug}`;
      const collection = await db.collection({ dbName, name: 'archive-monthly' });
      await collection.bulkWrite(ops);
    }));
};
