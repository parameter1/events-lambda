const Bowser = require('bowser');
const mongodb = require('./mongodb');

const db = mongodb();

const run = async () => {
  const collection = await db.collection({ dbName: 'p1-events-acbm-fcp', name: 'events' });
  const agents = await collection.distinct('ua');

  const browsers = {};
  const systems = {};
  agents.forEach((agent) => {
    const { browser, os } = Bowser.parse(agent);
    const { name, version } = browser;
    if (!browsers[name]) browsers[name] = new Set();
    if (!systems[os.name]) systems[os.name] = new Set();

    let major = 'unknown';
    if (version) {
      const parts = version.split('.');
      major = parts.shift();
    }

    browsers[name].add(major);
    systems[os.name].add(os.version);
  });
  console.log(browsers);
  console.log(systems);
  console.log(`Based on ${agents.length} agents`);

  await db.close();
};

run().catch((e) => setImmediate(() => { throw e; }));
