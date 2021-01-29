const { nanoid } = require('nanoid');
const AWS = require('aws-sdk');

const {
  QUEUE_URL,
  LL_BQ_QUEUE_URL,
  ENTITIES_QUEUE_URL,
} = process.env;

const sqs = new AWS.SQS();
const contentType = 'text/plain';
const requiredEventFields = ['slug', 'host', 'act', 'cat', 'ent', 'vis'];

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

const validate = (payload) => requiredEventFields.every((key) => payload[key]);

/**
 * Enabled tenants. If the incoming payload `slug` does not match
 * one of these tenants, an error will be thrown.
 */
const tenants = {
  acbm: true,
  randallreilly: true,
};

exports.handler = async (event, context) => {
  const { requestContext, queryStringParameters, body } = event;
  const { http } = requestContext;

  if (http.method === 'OPTIONS') return { statusCode: 200 };
  const version = queryStringParameters && queryStringParameters.version;

  if (body || (queryStringParameters && queryStringParameters.data)) {
    const payload = parse(body || queryStringParameters.data);
    if (!payload) return badRequest('Unable to parse JSON payload from data query parameter');
    if (!validate(payload)) return badRequest(`Invalid event payload provided. Must contain ${requiredEventFields.join(', ')}`);

    const { slug } = payload;
    if (!tenants[slug]) return badRequest(`The tenant slug '${slug}' is not enabled.`);

    if (version === '2') {
      const { ent } = payload;

      const message = {
        _id: nanoid(),
        ts: Date.now(),

        slug,
        host: payload.host,
        act: payload.act,
        cat: payload.cat,
        lab: payload.lab,
        ent: ent.id,
        vis: payload.vis,
        idt: payload.idt,

        ip: requestContext.http.sourceIp,
        ua: requestContext.http.userAgent,
        version,
      };

      await Promise.all([
        sqs.sendMessage({ QueueUrl: QUEUE_URL, MessageBody: JSON.stringify(message) }).promise(),
        sqs.sendMessage({ QueueUrl: ENTITIES_QUEUE_URL, MessageBody: JSON.stringify(ent) }).promise(),
        sqs.sendMessage({
          QueueUrl: LL_BQ_QUEUE_URL,
          MessageBody: JSON.stringify({
            ...message,
            entity: ent,
          }),
        }).promise(),
      ]);

      return {
        statusCode: 200,
        body: `OK (v${version})`,
        headers: { 'content-type': contentType },
      }
    }

    const message = JSON.stringify({
      _id: nanoid(),
      ts: Date.now(),

      slug,
      host: payload.host,
      act: payload.act,
      cat: payload.cat,
      ent: payload.ent,
      vis: payload.vis,
      idt: payload.idt,

      ip: requestContext.http.sourceIp,
      ua: requestContext.http.userAgent,
    });

    await Promise.all([
      sqs.sendMessage({ QueueUrl: QUEUE_URL, MessageBody: message }).promise(),
      sqs.sendMessage({ QueueUrl: LL_BQ_QUEUE_URL, MessageBody: message }).promise(),
    ]);
    return {
      statusCode: 200,
      body: 'OK',
      headers: { 'content-type': contentType },
    }
  }
  return badRequest('No body or data query parameter was provided.');
};
