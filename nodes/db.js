
const fs = require('fs').promises;

class MqttDb {
  static RED = null;

  constructor() {
    this.data = {};
    this.subs = { children: {}, cb: [] };
  }

  async dump(filePath) {
    const json = JSON.stringify(this.data, null, 2);
    await fs.writeFile(filePath, json, 'utf8');
  }

  async load(filePath) {
    const content = await fs.readFile(filePath, 'utf8');
    this.data = JSON.parse(content);
  }

  update(key, value, do_create_tree = false) {
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
        data[part] = {};
        // don't bother calling the subs here
      }
      data = data[part];
      if (subs && subs.children) {
        subs = subs.children[part];
      }
    }
    const lastpart = parts[i];
    if (data[lastpart] !== undefined || do_create_tree) {
      data[lastpart] = value;
      if (subs && subs.cb) {
        for (const cb in subs.cb) {
          cb(value);
        }
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
    for (; i < parts.length - 1; i++) {
      const part = parts[i];
      if (!subs[part]) {
        subs[part] = { children: {}, cb: [] };
      }
      const s = subs[part];
      subs = s.children;
    }
    subs.cbs.push(cb);
  }

  unsubscribe(key, cb) {
    let subs = this.subs;
    const parts = key.split(/[\/.]/);
    if (parts.length == 0) {
      return 0;
    }
    let i = 0;
    for (; i < parts.length - 1; i++) {
      const part = parts[i];
      const s = subs[part];
      if (!s) {
        return;
      }
      subs = s.children;
    }

    const cb_idx = sub.indexOf(cb);
    if (cb_idx > -1) {
      subs.splice(cb_idx, 1);
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
