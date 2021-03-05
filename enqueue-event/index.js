const AWS = require('aws-sdk'); // eslint-disable-line import/no-extraneous-dependencies
const { nanoid } = require('nanoid');
const hash = require('object-hash');
const Joi = require('@parameter1/joi');

const {
  ARCHIVE_QUEUE_URL,
  QUEUE_URL,
  LL_BQ_QUEUE_URL,
  ENTITIES_QUEUE_URL,
} = process.env;

const sqs = new AWS.SQS();
const contentType = 'text/plain';
const { isArray } = Array;

/**
 * Enabled tenants. If the incoming payload `slug` does not match
 * one of these tenants, an error will be thrown.
 */
const tenants = [
  'acbm',
  'allured',
  'indm',
  'pmmi',
  'randallreilly',
];

const entitySchema = Joi.object({
  id: Joi.string().trim().required(),
  name: Joi.string().trim().default(''),
  props: Joi.object(),
  refs: Joi.object(),
});

const eventSchema = Joi.object({
  act: Joi.string().trim().required(),
  cat: Joi.string().trim().required(),
  lab: Joi.string().trim().default(''),
  ent: entitySchema,
  props: Joi.object().default({}),
  ctx: entitySchema,
});

const rootSchema = Joi.object({
  slug: Joi.string().trim().valid(...tenants).required(),
  realm: Joi.string().trim().default(''),
  env: Joi.string().trim().default(''),
  host: Joi.alternatives().try(
    Joi.string().trim().replace(/:\d+$/, '').hostname(),
    Joi.string().trim().replace(/:\d+$/, '').ip({ version: ['ipv4'] }),
  ).required(),
  vis: Joi.string().trim().required(),
  idt: Joi.string().trim().default(''),
  events: Joi.array().items(eventSchema).required(),
});

const convertToMultiEvent = (payload) => {
  const fields = {
    root: Object.keys(rootSchema.describe().keys),
    event: Object.keys(eventSchema.describe().keys),
  };
  const event = fields.event.reduce((o, key) => {
    const value = payload[key];
    return { ...o, [key]: value };
  }, {});
  const root = fields.root.reduce((o, key) => {
    const value = payload[key];
    return { ...o, [key]: value };
  }, {});
  return { ...root, events: [event] };
};

const badRequest = (body) => ({
  statusCode: 400,
  body,
  headers: { 'content-type': contentType },
});

const parse = (data) => {
  try {
    return JSON.parse(decodeURIComponent(data));
  } catch (e) {
    return null;
  }
};

exports.handler = async (event = {}) => {
  const { requestContext = {}, body } = event;
  const { http = {} } = requestContext;

  if (http.method === 'OPTIONS') return { statusCode: 200 };
  if (!body) return badRequest('No body was provided.');
  let payload = parse(body);
  if (!payload) return badRequest('Unable to parse JSON payload from data query parameter');

  if (!isArray(payload.events)) {
    // convert legacy, single event payloads to multi.
    payload = convertToMultiEvent(payload);
  }

  // then validate standard schema
  const { value, error } = rootSchema.validate(payload);
  if (error) return badRequest(`Invalid event payload: ${error.message}`);

  // build the event and entity messages to be processed
  const { slug } = value;
  const now = Date.now();
  const { events, entities, bigQuery } = value.events.reduce((arrs, evt) => {
    const { ent, ctx, props } = evt;

    const eventMessage = {
      _id: nanoid(),
      ts: now,
      slug: value.slug,
      realm: value.realm,
      env: value.env,
      host: value.host,
      vis: value.vis,
      idt: value.idt,
      act: evt.act,
      cat: evt.cat,
      lab: evt.lab,
      ent: ent && ent.id ? ent.id : '',
      ctx: ctx && ctx.id ? ctx.id : '',
      props: { ...props, _id: hash(props, { algorithm: 'md5' }) },

      ip: http.sourceIp,
      ua: http.userAgent,
    };

    arrs.events.push(eventMessage);
    if (ent && ent.id) arrs.entities.push({ ...ent, slug });
    if (ctx && ctx.id) arrs.entities.push({ ...ctx, slug });

    if (slug === 'acbm' && ent && ent.id) {
      arrs.bigQuery.push({ ...eventMessage, entity: { ...ent, slug } });
    }
    return arrs;
  }, { events: [], entities: [], bigQuery: [] });

  await Promise.all([
    ...events.map((message) => sqs.sendMessage({
      QueueUrl: QUEUE_URL,
      MessageBody: JSON.stringify(message),
    }).promise()),
    ...events.map((message) => sqs.sendMessage({
      QueueUrl: ARCHIVE_QUEUE_URL,
      MessageBody: JSON.stringify(message),
    }).promise()),
    ...entities.map((message) => sqs.sendMessage({
      QueueUrl: ENTITIES_QUEUE_URL,
      MessageBody: JSON.stringify(message),
    }).promise()),
    ...bigQuery.map((message) => sqs.sendMessage({
      QueueUrl: LL_BQ_QUEUE_URL,
      MessageBody: JSON.stringify(message),
    }).promise()),
  ]);
  return {
    statusCode: 200,
    body: 'OK',
    headers: { 'content-type': contentType },
  };
};
