const { inspect } = require('util');
const { BigQuery } = require('@google-cloud/bigquery');
const path = require('path');

process.env.GOOGLE_APPLICATION_CREDENTIALS = path.resolve(__dirname, './bq-creds.json');
const bq = new BigQuery();

const splitEntityId = (entityId) => {
  const [namespace, id] = entityId.split('*');
  return { namespace, id };
};

const buildPrimarySection = (refs) => {
  if (!refs) return;
  const { primarySection } = refs;
  if (!primarySection) return;
  const { id, namespace } = splitEntityId(primarySection.id);
  return {
    id,
    namespace,
    name: primarySection.name,
  };
};

const buildCompany = (refs) => {
  if (!refs) return;
  const { company } = refs;
  if (!company) return;
  const { id, namespace } = splitEntityId(company.id);
  return {
    id,
    namespace,
    name: company.name,
  };
};

const buildCreatedBy = (refs) => {
  if (!refs) return;
  const { createdBy } = refs;
  if (!createdBy) return;
  const { props } = createdBy;
  const { id, namespace } = splitEntityId(createdBy.id);
  return {
    id,
    namespace,
    username: createdBy.name,
    ...(props && props.firstName && { firstName: props.firstName }),
    ...(props && props.lastName && { lastName: props.lastName }),
  };
};

const buildAuthors = (refs) => {
  if (!refs) return;
  const { authors } = refs;
  if (!authors || !Array.isArray(authors)) return;
  return authors.map((author) => {
    const { id, namespace } = splitEntityId(author.id);
    return { id, namespace, name: author.name };
  });
};

const handleContentEvent = ({ json, entity }) => {
  const ent = {
    ...json.entity,
    name: entity.name,
    ...(entity.props && { type: entity.props.type }),
    ...(entity.props && entity.props.published && { published: new Date(entity.props.published) }),
    primarySection: buildPrimarySection(entity.refs),
    company: buildCompany(entity.refs),
    createdBy: buildCreatedBy(entity.refs),
    authors: buildAuthors(entity.refs),
  };
  return { ...json, entity: ent };
};

const handleSectionEvent = ({ json, entity }) => {
  const ent = {
    ...json.entity,
    name: entity.name,
  };
  return { ...json, entity: ent };
};

exports.handler = async (event, context) => {
  const streamed = [];
  const content = [];
  const sections = [];

  event.Records.forEach((record) => {
    const event = JSON.parse(record.body);

    const [eNS, eID] = event.ent.split('*');
    const json = {
      _id: event._id,
      date: new Date(event.ts),
      host: event.host,
      action: event.act,
      category: event.cat,
      visitor_id: event.vis,
      entity: {
        id: eID,
        namespace: eNS,
      },
      ip: event.ip,
      ua: event.ua,
    };
    if (event.idt) {
      const [iNS, iID] = event.idt.split('*');
      json.identity = {
        id: iID.split('~')[0],
        namespace: iNS,
      };
    }

    if (event.version === '2') {
      const { entity } = event;
      delete json.category;

      if (event.cat === 'Content') {
        content.push({ insertId: event._id, json: handleContentEvent({ json, entity }) });
      } else if (event.cat === 'Website Section') {
        sections.push({ insertId: event._id, json: handleSectionEvent({ json, entity }) });
      }
    } else {
      streamed.push({ insertId: event._id, json })
    }
  });

  const promises = [];
  if (streamed.length) promises.push(bq.dataset('events').table('streamed').insert(streamed, { raw: true }));
  if (content.length) promises.push(bq.dataset('events').table('content').insert(content, { raw: true }));
  if (sections.length) promises.push(bq.dataset('events').table('sections').insert(sections, { raw: true }));
  await Promise.all(promises);
};
