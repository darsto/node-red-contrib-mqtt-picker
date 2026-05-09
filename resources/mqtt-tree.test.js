const assert = require("node:assert/strict");
const fs = require("node:fs");
const test = require("node:test");

eval(fs.readFileSync("resources/mqtt-tree.js") + "");

const ids = (nodes) => nodes.map((node) => node.id);

const find = (nodes, id) => {
  const stack = [...nodes];
  while (stack.length) {
    const node = stack.shift();
    if (node.id === id) {
      return node;
    }
    stack.push(...node.children);
  }
  return null;
};

test("basic", () => {
  const tree = MqttTree.new({
    other: { leaf: "outside stat" },
    stat: {
      device: {
        POWER: "ON",
        nested: { value: 42 },
        list: [1, 2],
        empty: null,
        off: false,
      },
    },
  });

  assert.deepEqual(ids(tree), ["other", "stat.device"]);
  assert.deepEqual(find(tree, "other.leaf"), {
    id: "other.leaf",
    value: "outside stat",
    children: [],
  });
  assert.deepEqual(find(tree, "stat.device.POWER"), {
    id: "stat.device.POWER",
    value: "ON",
    children: [],
  });
  assert.deepEqual(find(tree, "stat.device.nested.value"), {
    id: "stat.device.nested.value",
    value: 42,
    children: [],
  });
  assert.deepEqual(find(tree, "stat.device.list"), {
    id: "stat.device.list",
    value: [1, 2],
    children: [],
  });
  assert.deepEqual(find(tree, "stat.device.empty"), {
    id: "stat.device.empty",
    value: null,
    children: [],
  });
  assert.deepEqual(find(tree, "stat.device.off"), {
    id: "stat.device.off",
    value: false,
    children: [],
  });
});

test("nostat", () => {
  const tree = MqttTree.new({
    alpha: { one: 1 },
  });

  assert.deepEqual(ids(tree), ["alpha"]);
});

test("stat + tele", () => {
  const tree = MqttTree.new({
    stat: {
      plug: { POWER: "OFF" },
    },
    tele: {
      plug: { STATE: { Uptime: "1T00:00:00" }, SENSOR: { Power: 12 } },
    },
  });

  assert.deepEqual(ids(tree), ["stat.plug"]);
  assert.deepEqual(ids(find(tree, "stat.plug").children), [
    "stat.plug.POWER",
    "stat.plug.STATE",
    "stat.plug.SENSOR",
  ]);
  assert.equal(find(tree, "tele"), null);
  assert.equal(find(tree, "stat.plug.SENSOR.Power").value, 12);
});

test("tele + nostat", () => {
  const tree = MqttTree.new({
    tele: {
      sensor: { LWT: "Online", SENSOR: { Temperature: 21.5 } },
    },
  });

  assert.deepEqual(ids(tree), ["stat.sensor"]);
  assert.deepEqual(ids(find(tree, "stat.sensor").children), [
    "stat.sensor.LWT",
    "stat.sensor.SENSOR",
  ]);
});

test("stat + cmnd", () => {
  const tree = MqttTree.new({
    stat: {
      plug: {
        POWER: "OFF",
        Dimmer: 50,
        STATUS: "statuss",
      },
    },
    cmnd: {
      plug: {
        POWER: "",
        Dimmer: "",
      },
    },
  });

  assert.equal(find(tree, "stat.plug").writable, true);
  assert.equal(find(tree, "stat.plug.POWER").writable, true);
  assert.equal(find(tree, "stat.plug.POWER").value, "OFF");
  assert.equal(find(tree, "stat.plug.Dimmer").writable, true);
  assert.equal(find(tree, "stat.plug.STATUS").writable, undefined);
  assert.equal(find(tree, "stat.plug.STATUS").value, "statuss");
  assert.equal(find(tree, "cmnd"), null);
});

test("cmnd + nostat", () => {
  const tree = MqttTree.new({
    cmnd: {
      plug: {
        POWER: "",
        SetOption: { "19": "" },
      },
    },
  });

  assert.deepEqual(ids(tree), ["stat.plug"]);
  assert.equal(find(tree, "stat.plug").writable, true);
  assert.equal(find(tree, "stat.plug.POWER").writable, true);
  assert.deepEqual(find(tree, "stat.plug.POWER").children, []);
  assert.equal(find(tree, "stat.plug.SetOption").writable, true);
  assert.equal(find(tree, "stat.plug.SetOption.19").writable, true);
});

test("cmnd + nostat 2", () => {
  const tree = MqttTree.new({
    cmnd: {
      plug: {
        Rules: ["ON", "OFF"],
      },
    },
  });

  assert.equal(find(tree, "stat.plug.Rules").writable, true);
  assert.deepEqual(find(tree, "stat.plug.Rules").children, []);
});

test("stat + null cmnd", () => {
  const tree = MqttTree.new({
    stat: {
      plug: { POWER: "OFF" },
    },
    cmnd: {
      plug: { POWER: null },
    },
  });

  assert.equal(find(tree, "stat.plug.POWER").writable, true);
  assert.deepEqual(find(tree, "stat.plug.POWER").children, []);
});

test("stat + too generic cmnd", () => {
  const tree = MqttTree.new({
    stat: {
      plug: { POWER: "OFF" },
    },
    cmnd: {
      plug: "",
    },
  });

  assert.equal(find(tree, "stat.plug").writable, true);
  assert.equal(find(tree, "stat.plug.POWER").writable, undefined);
});

test("unknown cmnd", () => {
  const tree = MqttTree.new({
    stat: {
      existing: { POWER: "ON" },
    },
    cmnd: {
      missing: { POWER: "ON" },
    },
  });

  assert.deepEqual(ids(tree), ["stat.existing", "stat.missing"]);
  assert.equal(find(tree, "stat.missing").writable, true);
  assert.equal(find(tree, "stat.missing.POWER").writable, true);
  assert.equal(find(tree, "stat.missing").value, undefined);
});
