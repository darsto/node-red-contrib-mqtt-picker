const fs = require("fs");

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
      RED.httpAdmin.get(
        "/mqtt-db/data",
        RED.auth.needsPermission("mqtt-db.read"),
        (req, res) => {
          res.json(MqttDb.inst?.data || {});
        },
      );
      RED.httpAdmin.post(
        "/mqtt-db/data",
        RED.auth.needsPermission("mqtt-db.write"),
        (req, res) => {
          if (MqttDb.inst && req.body?.action == "remove" && req.body?.id) {
            MqttDb.inst.remove(req.body.id);
          }
          res.json(MqttDb.inst?.data || {});
        },
      );

      RED.httpNode.get("/mqtt-db/data", (req, res) => {
        res.json(MqttDb.inst?.data || {});
      });
      RED.httpNode.post("/mqtt-db/subscribe", async (req, res) => {
        const db = MqttDb.inst;
        if (!db) {
          res.statusCode = 500;
          res.end();
          return;
        }

        let topics;
        try {
          let body = req.body;
          if (body === undefined) {
            const chunks = [];
            for await (const chunk of req) {
              chunks.push(Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk));
            }
            body = Buffer.concat(chunks).toString("utf8");
          }

          if (Buffer.isBuffer(body)) {
            body = body.toString("utf8");
          }
          if (typeof body === "string") {
            try {
              body = JSON.parse(body);
            } catch (err) {
              throw new Error("Request body must be valid JSON");
            }
          }

          topics = Array.isArray(body) ? body : body?.topics;
          if (!Array.isArray(topics) || topics.length === 0) {
            throw new Error(
              "Request body must contain a non-empty topics array",
            );
          }
          if (
            topics.some((topic) =>
              typeof topic !== "string" || topic.length === 0
            )
          ) {
            throw new Error("Every topic must be a non-empty string");
          }
          topics = [...new Set(topics)];
        } catch (err) {
          if (req.aborted || res.destroyed || res.writableEnded) {
            return;
          }
          res.statusCode = err.statusCode || 400;
          res.setHeader("Content-Type", "application/json; charset=utf-8");
          res.end(JSON.stringify({ error: err.message }));
          return;
        }

        if (req.aborted || res.destroyed || res.writableEnded) {
          return;
        }

        res.statusCode = 200;
        res.setHeader("Content-Type", "application/x-ndjson; charset=utf-8");
        res.setHeader("Cache-Control", "no-cache, no-transform");
        res.setHeader("X-Accel-Buffering", "no");
        if (typeof res.flushHeaders === "function") {
          res.flushHeaders();
        }

        let closed = false;
        const subscriptions = [];
        const cleanup = () => {
          if (closed) {
            return;
          }
          closed = true;
          for (const [topic, cb] of subscriptions) {
            db.unsubscribe(topic, cb);
          }
        };
        const send = (topic, value, acked) => {
          if (closed || res.destroyed || res.writableEnded) {
            return;
          }
          try {
            res.write(JSON.stringify({ topic, value, acked }) + "\n");
            if (typeof res.flush === "function") {
              res.flush();
            }
          } catch (err) {
            cleanup();
            if (!res.destroyed && !res.writableEnded) {
              res.end();
            }
          }
        };

        for (const topic of topics) {
          subscriptions.push([topic, db.subscribe(topic, send)]);
        }

        req.on("aborted", cleanup);
        res.on("close", cleanup);
        res.on("error", cleanup);
        res.on("finish", cleanup);
      });
    }
    return MqttDb.inst;
  }

  dump(filePath) {
    const json = JSON.stringify(this.data, null, 2);
    fs.writeFileSync(filePath || this.path, json, "utf8");
  }

  load(filePath) {
    const content = fs.readFileSync(filePath || this.path, "utf8");
    this.data = MqttDb.normalize_data(JSON.parse(content));
  }

  update(key, value, do_create_tree = true, acked = true) {
    if (MqttDb.is_discovery_topic(key)) {
      return 0;
    }
    let data = this.data;
    let subs = this.subs;
    const gathered_subs = [...subs.cb];
    key = key.replaceAll("/", ".");
    const parts = this.split_key(key);
    if (parts.length == 0) {
      return 0;
    }
    let i = 0;
    for (; i < parts.length - 1; i++) {
      const part = parts[i];
      const d = data[part];
      if (typeof d !== "object" || !d || Array.isArray(d)) {
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
      if (is_plain_object(value)) {
        if (!is_plain_object(data[lastpart])) {
          data[lastpart] = {};
        }
        assign_recur(data[lastpart], value);
        call_subs_recur(gathered_subs, key, subs, value, data[lastpart], acked);
      } else {
        data[lastpart] = value;
        call_subs(gathered_subs, key, value, acked);
      }
    }
  }

  query(key) {
    if (MqttDb.is_discovery_topic(key)) {
      return;
    }
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
      if (typeof d !== "object" || !d || Array.isArray(d)) {
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
      if (typeof data !== "object" || !data || Array.isArray(data)) {
        return undefined;
      }
    }
    const lastpart = parts[i];
    return data[lastpart];
  }

  split_key(key) {
    return key.split(/[\/.]/);
  }

  static normalize_topic(topic) {
    const parts = String(topic).replaceAll("/", ".").split(".");
    if (["stat", "tele", "cmnd"].includes(parts[0])) {
      parts.shift();
    }
    return MqttDb.collapse_info_topic(parts).join(".");
  }

  static is_discovery_topic(topic) {
    const normalized = String(topic).replaceAll("/", ".");
    return normalized === "tasmota.discovery" ||
      normalized.startsWith("tasmota.discovery.");
  }

  static collapse_info_topic(parts) {
    const collapsed = [];
    for (const part of parts) {
      const previous = collapsed[collapsed.length - 1];
      const outer = /^INFO(\d+)$/.exec(previous || "");
      const inner = /^Info(\d+)$/.exec(part);
      if (outer && inner && outer[1] === inner[1]) {
        continue;
      }
      collapsed.push(part);
    }
    return collapsed;
  }

  static collapse_info_payload(topic, value) {
    const parts = String(topic).split(".");
    const info = /^INFO(\d+)$/.exec(parts[parts.length - 1] || "");
    if (!info || !is_plain_object(value)) {
      return value;
    }
    const keys = Object.keys(value);
    const wrapper = `Info${info[1]}`;
    if (!Object.prototype.hasOwnProperty.call(value, wrapper)) {
      return value;
    }
    if (keys.length === 1) {
      return value[wrapper];
    }
    if (is_plain_object(value[wrapper])) {
      const siblings = { ...value };
      delete siblings[wrapper];
      return assign_recur(clone_value(value[wrapper]), siblings);
    }
    return value;
  }

  static normalize_data(source) {
    if (!is_plain_object(source)) {
      return {};
    }

    const normalized = {};
    for (const prefix of ["cmnd", "tele", "stat"]) {
      if (is_plain_object(source[prefix])) {
        assign_recur(normalized, clone_value(source[prefix]));
      }
    }
    for (const [key, value] of Object.entries(source)) {
      if (!["cmnd", "tele", "stat"].includes(key)) {
        assign_recur(normalized, { [key]: clone_value(value) });
      }
    }

    if (is_plain_object(normalized.tasmota)) {
      delete normalized.tasmota.discovery;
      if (Object.keys(normalized.tasmota).length === 0) {
        delete normalized.tasmota;
      }
    }
    return collapse_info_data(normalized);
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

const call_subs_recur = (gathered_subs, key, subs, update_obj, obj, acked) => {
  for (const prop in update_obj) {
    const prop_subs = subs?.children[prop];
    const gathered_subs_length = gathered_subs.length;
    if (prop_subs) {
      gathered_subs.push(...prop_subs.cb);
    }
    const next_key = key + "." + prop;
    const next_update_obj = update_obj[prop];
    const next_obj = obj[prop];
    call_subs(gathered_subs, next_key, next_obj, acked);
    if (
      next_update_obj && typeof next_update_obj === "object" &&
      !Array.isArray(next_update_obj)
    ) {
      call_subs_recur(
        gathered_subs,
        next_key,
        prop_subs,
        next_update_obj,
        next_obj,
        acked,
      );
    }
    // restore pre-recursion state
    gathered_subs.length = gathered_subs_length;
  }
};

const call_subs = (gathered_subs, key, value, acked) => {
  for (const cb of gathered_subs) {
    cb(key, value, acked);
  }
};

const assign_recur = (dst, src) => {
  Object.keys(src).forEach((key) => {
    const s_val = src[key];
    const d_val = dst[key];
    dst[key] =
      is_plain_object(d_val) && is_plain_object(s_val)
        ? assign_recur(d_val, s_val)
        : s_val;
  });
  return dst;
};

const is_plain_object = (value) =>
  value !== null && typeof value === "object" && !Array.isArray(value);

const clone_value = (value) => {
  if (Array.isArray(value)) {
    return value.map(clone_value);
  }
  if (is_plain_object(value)) {
    return Object.fromEntries(
      Object.entries(value).map(([key, child]) => [key, clone_value(child)]),
    );
  }
  return value;
};

const collapse_info_data = (value, parent_key = "") => {
  if (Array.isArray(value)) {
    return value.map((child) => collapse_info_data(child));
  }
  if (!is_plain_object(value)) {
    return value;
  }

  const result = {};
  for (const [key, child] of Object.entries(value)) {
    result[key] = collapse_info_data(child, key);
  }

  const info = /^INFO(\d+)$/.exec(parent_key);
  if (info) {
    const wrapper = `Info${info[1]}`;
    const keys = Object.keys(result);
    if (keys.length === 1 && keys[0] === wrapper) {
      return result[wrapper];
    }
    if (
      Object.prototype.hasOwnProperty.call(result, wrapper) &&
      is_plain_object(result[wrapper])
    ) {
      const wrapper_value = result[wrapper];
      delete result[wrapper];
      return assign_recur(wrapper_value, result);
    }
  }
  return result;
};

module.exports = MqttDb;
