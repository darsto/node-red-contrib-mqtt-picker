const MqttDb = require("../nodes/db");

module.exports = function setup(patterns) {
  const types = {};
  const RED = { nodes: {
    createNode(node) {
      node.handlers = {};
      node.sent = [];
      node.errors = [];
      node.on = (event, cb) => { node.handlers[event] = cb; };
      node.send = (msg) => { node.sent.push(msg); node.forward?.(msg); };
      node.error = (err) => node.errors.push(err);
      node.status = (status) => { node.lastStatus = status; };
    },
    registerType(name, ctor) { types[name] = ctor; },
  } };
  MqttDb.inst = new MqttDb();
  for (const file of ["subscriber", "in", "out", "call"]) require("../nodes/" + file)(RED);
  const make = (type, config = {}) => new types["mqtt-db-" + type](config);
  const bridge = make("subscriber", { fullTopics: patterns });
  return { db: MqttDb.inst, bridge, make };
};
