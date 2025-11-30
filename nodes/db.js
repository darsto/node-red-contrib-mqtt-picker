
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
      RED.httpAdmin.post("/mqtt-db/data", (req, res) => {
        if (MqttDb.inst && req.body?.action == "remove" && req.body?.id) {
          MqttDb.inst.remove(req.body.id);
        }
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
    key = key.replaceAll('/', '.');
    const parts = key.split('.');
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
      if (typeof value === "object") {
        if (data[lastpart] === undefined) {
          data[lastpart] = {};
        }
        Object.assign(data[lastpart], value);
        call_subs_recur(this.sub_all, key, subs, value, acked);
      } else {
        const number = Number(value);
        const num_or_val = isNaN(number) ? value : number;
        data[lastpart] = num_or_val;
        call_subs(this.sub_all, subs, key, num_or_val, acked);
      }
    }
  }

  remove(key) {
    let data = this.data;
    const parts = key.split(/[\/.]/);
    if (parts.length == 0) {
      return 0;
    }
    let i = 0;
    for (; i < parts.length - 1; i++) {
      const part = parts[i];
      const d = data[part];
      if (typeof d !== 'object' || !d || Array.isArray(d)) {
        return;
      }
      data = data[part];
    }
    const lastpart = parts[i];
    if (data[lastpart] !== undefined) {
      delete data[lastpart];
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

const call_subs_recur = (sub_all, key, subs, update_obj, acked) => {
  for (const prop in update_obj) {
    const prop_subs = subs?.children[prop];
    const next_key = key + "." + prop;
    const next_obj = update_obj[prop];
    call_subs(sub_all, prop_subs, next_key, next_obj, acked);
    if (next_obj && typeof next_obj === 'object' && !Array.isArray(next_obj)) {
      call_subs_recur(sub_all, next_key, prop_subs, next_obj, acked);
    }
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
