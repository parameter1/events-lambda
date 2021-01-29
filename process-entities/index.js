const { inspect } = require('util');
const mongodb = require('./mongodb');

const db = mongodb();
const { keys } = Object;
const { isArray } = Array;
const isObject = (v) => v && typeof v === 'object';

const createOpsFor = (doc, ops = []) => {
  const { props, refs } = doc;
  const name = doc.name ? `${doc.name}`.trim() : doc.name;
  const filter = { _id: doc.id };
  const [namespace, id] = doc.id.split('*');

  const $setOnInsert = {
    ...filter,
    entity: { id, namespace },
    createdAt: new Date(),
  };
  const $set = {
    // set props when not an empty object
    ...(typeof props === 'object' && keys(props).length && { props }),
    // set name when not undefined and has a value
    ...(name && { name }),
    updatedAt: new Date(),
  };
  const $unset = {
    // unset props when an object _is_ provided but is empty
    // sending a falsy props value will _not_ unset what was previously provided.
    ...(typeof props === 'object' && !keys(props).length && { props: 1 }),
    // unset the name when _not_ undefined, but is falsy value
    ...(typeof name !== 'undefined' && !name && { name: 1 }),
  };

  if (isObject(refs)) {
    keys(refs).forEach((key) => {
      const dbField = `refs.${key}`;
      const ref = refs[key];
      if (!ref) {
        // unset falsy values
        $unset[dbField] = 1;
      } else if (isArray(ref)) {
        if (!ref.length) {
          // unset empty arrays
          $unset[dbField] = 1;
        } else {
          // handle ref manies and create update ops.
          $set[dbField] = ref.map((entity) => {
            if (!entity.id) throw new Error('No reference value id was provided.');
            createOpsFor(entity, ops);
            return entity.id;
          });
        }
      } else if (isObject(ref)) {
        // handle ref one and create update ops.
        if (!ref.id) throw new Error('No reference id was provided.');
        $set[dbField] = ref.id;
        createOpsFor(ref, ops);
      }
    });
  }
  const update = {
    $setOnInsert,
    $set,
    ...(keys($unset).length && { $unset }),
  };
  ops.push({ updateOne: { filter, update, upsert: true } });
  return ops;
};

exports.handler = async (event, context = {}) => {
  // see https://docs.atlas.mongodb.com/best-practices-connecting-to-aws-lambda/
  context.callbackWaitsForEmptyEventLoop = false;

  const opsMap = event.Records.reduce((map, record) => {
    const doc = JSON.parse(record.body);
    const { slug } = doc;
    if (!map.has(slug)) map.set(slug, []);
    map.get(slug).push(...createOpsFor(doc));
    return map;
  }, new Map());

  /**
   * Convert map into an array of bulk write operations per tenant.
   */
  await Promise.all(Array.from(opsMap, ([slug, ops]) => ({ slug, ops })).map(async ({ slug, ops }) => {
    const dbName = `p1-events-${slug}`;
    const collection = await db.collection({ dbName, name: 'entities' });
    await collection.bulkWrite(ops);
  }));
};
