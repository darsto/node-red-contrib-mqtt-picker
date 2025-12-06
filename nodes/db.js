
const fs = require('fs');

class MqttDb {
  static RED = null;
  static inst = null;

  constructor() {
    this.data = {};
    this.subs = { children: {}, cb: [] };
  }

  static instance(RED) {
    if (!MqttDb.inst) {
      MqttDb.RED = RED;
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
    const gathered_subs = [...subs.cb];
    key = key.replaceAll('/', '.');
    const parts = this.split_key(key);
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
      if (subs) {
        gathered_subs.push(...subs.cb);
      }
    }
    const lastpart = parts[i];
    if (data[lastpart] !== undefined || do_create_tree) {
      subs = subs?.children?.[lastpart];
      if (subs) {
        gathered_subs.push(...subs.cb);
      }
      if (typeof value === "object") {
        if (data[lastpart] === undefined) {
          data[lastpart] = {};
        }
        Object.assign(data[lastpart], value);
        call_subs_recur(gathered_subs, key, subs, data[lastpart], acked);
      } else {
        const number = Number(value);
        const num_or_val = isNaN(number) ? value : number;
        data[lastpart] = num_or_val;
        call_subs(gathered_subs, key, num_or_val, acked);
      }
    }
  }

  query(key) {
    call_subs(this.subs.cb, key, "", false);
  }

  remove(key) {
    let data = this.data;
    const parts = this.split_key(key);
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
    const parts = this.split_key(key);
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
    return cb;
  }

  unsubscribe(key, cb) {
    let subs = this.subs;
    const parts = this.split_key(key);
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
    const parts = this.split_key(key);
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

  split_key(key) {
    const parts = key.split(/[\/.]/);
    if (parts.length >= 3) {
      const tele_topics = ["LWT", "INFO1", "INFO2", "INFO3", "STATE", "SENSOR"];
      if (parts[0] == "tele" && !tele_topics.includes(parts[2])) {
        // best guess
        parts[0] = "stat";
      } else if (parts[0] == "stat" && tele_topics.includes(parts[2])) {
        parts[0] = "tele";
      }
    }
    return parts;
  }

  static process_resp(val) {
    if (val === "ON" || val === "Online") {
      val = true;
    } else if (val === "OFF" || val === "Offline") {
      val = false;
    }
    return val;
  }
}

const call_subs_recur = (gathered_subs, key, subs, update_obj, acked) => {
  for (const prop in update_obj) {
    const prop_subs = subs?.children[prop];
    const gathered_subs_length = gathered_subs.length;
    if (prop_subs) {
      gathered_subs.push(...prop_subs.cb);
    }
    const next_key = key + "." + prop;
    const next_obj = update_obj[prop];
    call_subs(gathered_subs, next_key, next_obj, acked);
    if (next_obj && typeof next_obj === 'object' && !Array.isArray(next_obj)) {
      call_subs_recur(gathered_subs, next_key, prop_subs, next_obj, acked);
    }
    // restore pre-recursion state
    gathered_subs.length = gathered_subs_length;
  }
}

const call_subs = (gathered_subs, key, value, acked) => {
  for (const cb of gathered_subs) {
    cb(key, value, acked);
  }
}

module.exports = MqttDb;
