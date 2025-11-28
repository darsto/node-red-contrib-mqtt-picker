
const fs = require('fs');

class MqttDb {
  static RED = null;
  static inst = null;

  constructor() {
    this.data = {};
    this.subs = { children: {}, cb: [] };
    this.sub_all = null;
  }

  static instance(RED) {
    if (!MqttDb.inst) {
      MqttDb.inst = new MqttDb();
      const path = RED.settings.userDir + "/mqttdb.json";
      MqttDb.inst.path = path;
      if (fs.existsSync(path)) {
        MqttDb.inst.load();
      }
      setInterval(() => {
        MqttDb.inst.dump();
      }, 1000 * 120);
      RED.httpAdmin.get("/mqtt-db/data", (req, res) => {
        res.json(MqttDb.inst?.data || {});
      });
    }
    return MqttDb.inst;
  }

  dump(filePath) {
    const json = JSON.stringify(this.data, null, 2);
    fs.writeFileSync(filePath || this.path, json, 'utf8');
  }

  load(filePath) {
    const content = fs.readFileSync(filePath || this.path, 'utf8');
    this.data = JSON.parse(content);
  }

  update(key, value, do_create_tree = true, acked = true) {
    let data = this.data;
    let subs = this.subs;
    const parts = key.split(/[\/.]/);
    if (parts.length == 0) {
      return 0;
    }
    let i = 0;
    for (; i < parts.length - 1; i++) {
      const part = parts[i];
      const d = data[part];
      if (typeof d !== 'object' || !d || Array.isArray(d)) {
        if (!do_create_tree) {
          return;
        }
        data[part] = {};
        // don't bother calling the subs here
      }
      data = data[part];
      subs = subs?.children?.[part];
    }
    const lastpart = parts[i];
    if (data[lastpart] !== undefined || do_create_tree) {
      subs = subs?.children?.[lastpart];
      let parsed_val = null;
      try {
        if (value[0] == "{") {
          parsed_val = JSON.parse(value);
        }
      } catch (e) {};
      if (parsed_val) {
        if (data[lastpart] === undefined) {
          data[lastpart] = {};
        }
        Object.assign(data[lastpart], parsed_val);
        for (const prop in parsed_val) {
          const prop_subs = subs?.children[prop];
          call_subs(this.sub_all, prop_subs, key + "." + prop, data[lastpart][prop], acked);
        }
      } else {
        const number = Number(value);
        const num_or_val = isNaN(number) ? value : number;
        data[lastpart] = num_or_val;
        call_subs(this.sub_all, subs, key, num_or_val, acked);
      }
    }
  }

  subscribe(key, cb) {
    let subs = this.subs;
    const parts = key.split(/[\/.]/);
    if (parts.length == 0) {
      return 0;
    }
    let i = 0;
    for (; i < parts.length; i++) {
      const part = parts[i];
      if (!subs.children[part]) {
        subs.children[part] = { children: {}, cb: [] };
      }
      subs = subs.children[part];
    }
    subs.cb.push(cb);
  }

  unsubscribe(key, cb) {
    let subs = this.subs;
    const parts = key.split(/[\/.]/);
    if (parts.length == 0) {
      return 0;
    }
    let i = 0;
    for (; i < parts.length; i++) {
      const part = parts[i];
      subs = subs.children[part];
      if (!subs) {
        return;
      }
    }

    const cb_idx = subs.cb.indexOf(cb);
    if (cb_idx > -1) {
      subs.cb.splice(cb_idx, 1);
    }
  }

  get(key) {
    let data = this.data;
    const parts = key.split(/[\/.]/);
    if (parts.length == 0) {
      return undefined;
    }
    let i = 0;
    for (; i < parts.length - 1; i++) {
      data = data[parts[i]];
      if (typeof data !== 'object' || !data || Array.isArray(data)) {
        return undefined;
      }
    }
    const lastpart = parts[i];
    return data[lastpart];
  }
}

const call_subs = (sub_all, subs, key, value, acked) => {
  if (subs && subs.cb) {
    for (const cb of subs.cb) {
      cb(key, value, acked);
    }
  }
  if (sub_all) {
    sub_all(key, value, acked);
  }
}

module.exports = MqttDb;
