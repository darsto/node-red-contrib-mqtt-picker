const assert = require('assert');
const MqttDb = require('./db');

const db = new MqttDb();
db.update("aa.bb", "val1");
db.update("aa.cc", "val2");
db.update("aa.dd.11", "val3");
db.update("aa.dd.22", "val4");
db.update("aa.dd.33", "42");
db.update("aa.dd.44", '{}');
db.update("aa.dd.44", '{"a":43}');
db.update("aa.dd.44.a", '44');
db.subscribe("aa.dd.44", (topic, val) => {
    console.log("aa.dd.44"); // should be only called once
});
db.subscribe("aa.dd.44.b", (topic, val) => {
    console.log("aa.dd.44.b");
});
db.update("aa.dd.44", "val5");
db.update("aa.dd.44", '{"b":45}');
db.dump("dump.json");
