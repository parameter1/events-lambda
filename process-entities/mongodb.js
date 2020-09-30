const MongoDBClient = require('@parameter1/mongodb/client');

module.exports = () => new MongoDBClient({ url: process.env.MONGO_DSN });
