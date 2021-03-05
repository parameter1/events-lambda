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
];

const db = mongodb();

exports.handler = async (event, context) => {
  // see https://docs.atlas.mongodb.com/best-practices-connecting-to-aws-lambda/
  context.callbackWaitsForEmptyEventLoop = false;

  const opsMap = new Map();
  const visitorMap = new Map();

  /**
   * Map all records to the appropriate tenant slug.
   */
  event.Records.forEach((record) => {
    const doc = JSON.parse(record.body);
    const { slug } = doc;
    if (!opsMap.has(slug)) opsMap.set(slug, []);
    if (!visitorMap.has(slug)) visitorMap.set(slug, []);

    // replace the timestamp with a Date
    const date = new Date(doc.ts);
    // calculate the month.
    const month = dayjs(date).tz('UTC').startOf('month').toDate();

    // create the formatted event that will be saved
    const formatted = keys.reduce((o, key) => {
      const value = (doc[key] || '').trim();
      return { ...o, [key]: value };
    }, {});
    formatted.props = doc.props && doc.props._id
      ? doc.props
      : { _id: objectHash({}, { algorithm: 'md5' }) };

    // format the data to hash
    const toHash = Object.keys(formatted).reduce((o, key) => {
      const value = key === 'props' ? formatted.props._id : formatted[key];
      return { ...o, [key]: value };
    }, {});

    // create the hash from the hashable object
    // this will become the unique event id
    const hash = objectHash(toHash);

    visitorMap.get(slug).push({
      updateOne: {
        filter: { month, hash },
        update: {
          $addToSet: {
            // create a unique set of visitors for the provided event and month
            // use the identity, when present, otherwise use the visitor id
            visitors: doc.idt ? doc.idt : doc.vis,
          },
          // add first and last seen at dates
          $min: { firstSeenAt: date },
          $max: { lastSeenAt: date },
        },
        upsert: true,
      },
    });

    // the upsert criteria
    const filter = { month, 'event.hash': hash };
    // the update settings
    const update = {
      $setOnInsert: {
        month,
        event: { ...formatted, hash },
      },
      $inc: { count: 1 }, // increment the event count
      // @todo replace the `visitors` array with a raw count
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
    opsMap.get(slug).push(op);
  });

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
