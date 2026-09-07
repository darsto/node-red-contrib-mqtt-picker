import fs = require("node:fs");

class MqttDb {
  // A tree of MQTT properties built off MQTT topics
  public declare tree: Record<string, MqttDbNode>;

  // Publisher supplied by the active mqttdb-subscriber node.
  // MqttDb exists even without any mqttdb-subscriber node.
  // NodeRED inits nodes in any order, so mqttdb-subscriber may
  // effectively perform a late init - tracked by this var
  declare publisher: ((message: { topic: string; payload: any }) => void) | null;

  // Tasmota-style Full Topic patterns - i.e. %prefix%/%topic%
  declare full_topics: Pattern[];

  declare json_path?: string;
  declare json_dump_interval: NodeJS.Timeout | null; // 2 mins
  declare on_error: (err: unknown) => void;
  public static inst: MqttDb | null = null;

  // New empty db
  public constructor() {
    this.tree = Object.create(null);
    this.full_topics = MqttDb.patterns();
    this.publisher = null;
    this.json_dump_interval = null;
    this.on_error = () => { };
  }

  // Return the shared database and register its HTTP endpoints.
  public static instance(RED: any): MqttDb {
    if (!MqttDb.inst) {
      const db = new MqttDb();
      db.json_path = RED.settings.userDir + "/mqttdb.json";
      db.on_error = (err) => RED.log.error(err instanceof Error ? err.message : String(err));
      if (fs.existsSync(db.json_path)) db.load();
      MqttDb.inst = db;
      RED.httpAdmin.get("/mqtt-db/data", RED.auth.needsPermission("mqtt-db.read"), (_req: any, res: any) => res.json(db.data()));
      RED.httpAdmin.post("/mqtt-db/data", RED.auth.needsPermission("mqtt-db.write"), (req: any, res: any) => {
        try {
          if (req.body?.action !== "remove") throw new Error("Expected action: remove");
          db.remove(req.body.id);
          db.dump();
          res.json(db.data());
        } catch (err) { res.status(400).json({ error: err instanceof Error ? err.message : String(err) }); }
      });
      RED.httpNode.get("/mqtt-db/data", (_req: any, res: any) => res.json(db.data()));
      RED.httpNode.post("/mqtt-db/subscribe", (req: any, res: any) => db.stream(req, res));
    }
    return MqttDb.inst;
  }

  // Return a detached public view of the tree.
  public data() {
    return Object.fromEntries(Object.entries(this.tree).flatMap(([key, node]) => {
      const value = node.data();
      return value === undefined ? [] : [[key, value]];
    }));
  }

  // Read a detached value from a picker path.
  public get(key: string): any {
    const { parts } = MqttTopicParser.parse(key);
    const node = MqttDbNode.at(this.tree, parts);
    return node?.data();
  }

  // Subscribe to changes at a picker path.
  public subscribe(key: string, cb: Subscription["cb"]): () => boolean {
    const parsed = MqttTopicParser.parse(key, true);
    const parts = [];
    let children = this.tree;
    let node: MqttDbNode;
    for (const input of parsed.parts) {
      const part = storedKey(children, input);
      if (!children[part]) put(children, part, new MqttDbNode());
      node = children[part];
      children = node.children;
      parts.push(part);
    }
    const sub: Subscription = { ...parsed, parts, cb, node: node! };
    node!.subscribers.add(sub);
    return () => sub.node.subscribers.delete(sub);
  }

  // MQTT publish
  public publish(key: string, value: any, fullTopic?: string): void {
    if (!this.publisher) throw new Error("No mqttdb subscriber is connected");

    const parts = MqttDbNode.resolve(this.tree, MqttTopicParser.parse(key).parts);
    const finalFullTopic = fullTopic ? { fullTopic, index: 0 } : MqttDbNode.findFullTopic(this.tree, parts);
    const relativeTopic = MqttTopicParser.format(parts.slice(finalFullTopic?.index || 0));
    const topic = MqttDb.buildPublishTopic(relativeTopic, finalFullTopic?.fullTopic || "%topic%/");
    const payload = json(value);
    this.publisher({ topic, payload });
  }

  // MQTT publish a query and wait for value
  public query(key: string, { timeout = 5000, signal }: { timeout?: number; signal?: AbortSignal } = {}): Promise<any> {
    if (!Number.isFinite(timeout) || timeout <= 0) return Promise.reject(new Error("Query timeout must be positive"));
    if (signal?.aborted) return Promise.reject(new Error("Query cancelled"));

    const parts = MqttDbNode.resolve(this.tree, MqttTopicParser.parse(key).parts);
    const { fullTopic } = MqttDbNode.findFullTopic(this.tree, parts) ?? {};
    if (!fullTopic || !MqttDb.pattern(fullTopic).prefixed) {
      // no %prefix%/
      return Promise.reject(new Error("Active queries require a FullTopic with %prefix%"));
    }
    return new Promise((resolve, reject) => {
      let finished = false;
      let unsubscribe: () => unknown = () => { };
      let timer: NodeJS.Timeout | undefined;
      const cancel = () => finish(new Error("Query cancelled"));
      const finish = (err?: Error | null, value?: any) => {
        if (finished) return;
        finished = true;
        clearTimeout(timer);
        unsubscribe();
        signal?.removeEventListener("abort", cancel);
        if (err) reject(err); else resolve(value);
      };
      unsubscribe = this.subscribe(MqttTopicParser.format(parts), (_topic, value, deleted) => {
        if (!deleted) finish(null, value);
      });
      signal?.addEventListener("abort", cancel, { once: true });
      timer = setTimeout(() => finish(new Error("MQTT query timed out: " + key)), timeout);
      try { this.publish(key, ""); } catch (err) { finish(err instanceof Error ? err : new Error(String(err))); }
    });
  }

  // Apply an incoming MQTT value into the tree.
  public receive(topic: string, payload: any): boolean {
    validateMqttTopic(topic);

    const { path, topicIndex, fullTopic, prefix, prefixed, suffix: matchedSuffix } = this.match(topic);
    if (prefix?.toLowerCase() === "cmnd") return false;

    const name = path[0];
    const routePath = path.slice(0, topicIndex + 1);
    const existingRoute = MqttDbNode.at(this.tree, routePath);
    if (existingRoute?.fullTopic && existingRoute.fullTopic !== fullTopic) {
      // stat.device.POWER and device.stat.POWER
      throw new Error("Conflicting MQTT Topics for device " + path[topicIndex]
        + ": " + existingRoute.fullTopic + " and " + fullTopic);
    }

    // strip RESULT/SENSOR/STATUS/INFOx
    let suffix = matchedSuffix;
    let value = json(payload, prefixed, new Set<any>(), 0, true);
    if (prefixed) {
      const wrapper = suffix[0]?.toUpperCase();
      if (wrapper === "RESULT" || wrapper === "SENSOR") {
        suffix = suffix.slice(1);
        if (!suffix.length && !object(value)) {
          throw new Error("Device root/RESULT or SENSOR payload must be a JSON object");
        }
      } else if (suffix.length === 1 &&
        (wrapper.startsWith("INFO") || wrapper === "STATUS") &&
        object(value) && Object.keys(value).length === 1) {
        value = Object.values(value)[0];
      }
    }

    // Patch the tree + gather subscribers to notify
    const relativePath = [...path.slice(1), ...suffix];
    let stored = value;
    for (const part of relativePath.reverse()) stored = { [part]: stored };
    stored = json(stored);
    const changed = new Map<string, string[]>();
    const subscriptions = new Set<Subscription>();
    merge(this.tree, name, stored, [name], changed, subscriptions);

    // Attach the matched route to its device node and update object route metadata.
    const storedRoutePath = MqttDbNode.resolve(this.tree, routePath);
    const route = MqttDbNode.at(this.tree, storedRoutePath)!;
    route.fullTopic = fullTopic;
    if (object(route.value)) {
      merge(route.children, "_update_ts", Date.now(), [...storedRoutePath, "_update_ts"], changed, subscriptions);
    }

    // Notify subscribers - first gather them, then notify 1 by 1
    const paths = [...changed.values()];
    const deliveries: [Subscription, string, any, boolean][] = [];
    for (const sub of subscriptions) {
      if (sub.wildcard) {
        for (const path of paths) {
          if (!starts(path, sub.parts) || path.length <= sub.parts.length) continue;
          const current = MqttDbNode.at(this.tree, path);
          const next = current?.data();
          if (object(next) && Object.keys(next).length) continue;
          deliveries.push([sub, MqttTopicParser.format(path), next === undefined ? null : next, next === undefined]);
        }
      } else if (paths.some((path) => starts(path, sub.parts) || starts(sub.parts, path))) {
        const current = MqttDbNode.at(this.tree, sub.parts);
        const next = current?.data();
        deliveries.push([sub, MqttTopicParser.format(sub.parts), next === undefined ? null : next, next === undefined]);
      }
    }
    for (const [sub, path, next, deleted] of deliveries) {
      if (!sub.node.subscribers.has(sub)) continue;
      try { sub.cb(path, next, deleted); } catch (err) { this.on_error(err); }
    }
    return true;
  }

  // Remove a picker path without publishing a message.
  public remove(key: string): boolean {
    const { parts } = MqttTopicParser.parse(key);
    let children = this.tree;
    for (const input of parts.slice(0, -1)) {
      const part = storedKey(children, input);
      const node = children[part];
      if (!node?.children) return false;
      children = node.children;
    }
    const last = storedKey(children, parts.at(-1)!);
    if (!own(children, last)) return false;
    const placeholder = new MqttDbNode();
    const subscriptions = new Set<Subscription>();
    const keep = children[last].copyMetadata(placeholder, subscriptions, false);
    if (keep) put(children, last, placeholder); else delete children[last];
    return true;
  }

  // Initialize the MQTT publisher and Full Topic patterns.
  public init(publish: (message: { topic: string; payload: any }) => void, full_topics?: string | string[]): void {
    if (this.publisher) throw new Error("Only one mqttdb subscriber may publish for this database");
    const parsed = MqttDb.patterns(full_topics);
    this.publisher = publish;
    this.full_topics = parsed;
    if (this.json_path) {
      this.json_dump_interval = setInterval(() => {
        try { this.dump(); } catch (err) { this.on_error(err); }
      }, 120000);
      this.json_dump_interval.unref();
    }
  }

  // Deinitialize the MQTT publisher.
  public deinit(): void {
    if (!this.publisher) return;
    this.publisher = null;
    if (this.json_dump_interval) clearInterval(this.json_dump_interval);
    this.json_dump_interval = null;
    if (this.json_path) this.dump();
  }

  // Persist the structured tree as a v3 database.
  public dump(filePath = this.json_path): void {
    if (!filePath) return;
    const temporary = filePath + ".tmp";
    fs.writeFileSync(temporary, JSON.stringify({ version: 3, tree: this.tree }, null, 2), "utf8");
    fs.renameSync(temporary, filePath);
  }

  // Replace memory with a validated v3 database.
  public load(filePath: string = this.json_path!): void {
    const source = JSON.parse(fs.readFileSync(filePath, "utf8"));
    if (object(source) && Number.isInteger(source.version) && source.version >= 0 && source.version < 3) {
      const oldPath = filePath + ".v" + source.version;
      fs.rmSync(oldPath, { force: true });
      fs.renameSync(filePath, oldPath);
      this.tree = Object.create(null);
      return;
    }
    if (!object(source) || source.version !== 3 || !object(source.tree)) {
      throw new Error("Unsupported MQTT database version");
    }
    const parse = (input: unknown, convert = false, depth = 0): MqttDbNode => {
      if (depth > 100 || !object(input) || Object.keys(input).some((key) => !["children", "value", "route"].includes(key))) {
        throw new Error("Invalid MQTT tree node");
      }
      let route;
      if (own(input, "route")) {
        const pattern = MqttDb.pattern(input.route);
        route = pattern.fullTopic;
        convert = pattern.prefixed;
      }
      const hasChildren = own(input, "children"), hasValue = own(input, "value");
      if ((hasChildren && !object(input.children)) ||
        (hasValue && object(input.value) && Object.keys(input.value).length) ||
        (!hasValue && !hasChildren && !route)) {
        throw new Error("Invalid MQTT tree node");
      }
      const node = new MqttDbNode(hasValue ? json(input.value, convert, new Set<any>(), 0, true) : undefined);
      if (hasChildren) {
        for (const [inputKey, child] of Object.entries(input.children)) {
          const key = storedKey(node.children, inputKey);
          if (own(node.children, key)) throw new Error("Duplicate case-insensitive MQTT property: " + inputKey);
          put(node.children, key, parse(child, convert, depth + 1));
        }
      }
      if (route) node.fullTopic = route;
      return node;
    };
    const tree = Object.create(null);
    for (const [inputName, input] of Object.entries(source.tree)) {
      validateMqttTopic(inputName);
      if (inputName.includes("/")) throw new Error("Invalid tree root: " + inputName);
      const name = storedKey(tree, inputName);
      if (own(tree, name)) throw new Error("Duplicate case-insensitive MQTT property: " + inputName);
      put(tree, name, parse(input));
    }
    this.tree = tree;
  }

  // mydev.topic + %prefix%/%topic%/ => cmnd/mydev/topic
  public static buildPublishTopic(key: string, fullTopic: string): string {
    const parts = MqttTopicParser.parse(key).parts.map(normalizeKey);
    MqttDb.pattern(fullTopic);
    const suffix = parts.slice(1).join("/");
    const topic = fullTopic.slice(0, -1)
      .replace("%prefix%", "cmnd")
      .replace("%topic%", parts[0]) + (suffix ? "/" + suffix : "");
    validateMqttTopic(topic);
    return topic;
  }

  // Parse and validate configured Full Topic patterns.
  public static patterns(input: string | string[] = ["%prefix%/%topic%/", "%topic%/"]): Pattern[] {
    if (typeof input === "string") {
      input = input.trim();
      input = input.startsWith("[") ? JSON.parse(input) : input.split(/\r?\n/).filter((p) => p.trim()).map((p) => p.trim());
    }
    if (!Array.isArray(input) || !input.length) throw new Error("Configure at least one Full Topic pattern");
    return [...new Map(input.map((pattern) => [pattern.toLowerCase(), pattern])).values()].map(MqttDb.pattern);
  }

  // Parse and validate one Full Topic pattern.
  public static pattern(fullTopic: string): Pattern {
    if (typeof fullTopic !== "string" || !fullTopic.endsWith("/")) {
      throw new Error("Full Topic must end in /");
    }
    const parts = fullTopic.slice(0, -1).split("/");
    if (parts.filter((p) => p === "%topic%").length !== 1 ||
      parts.filter((p) => p === "%prefix%").length > 1) {
      throw new Error("Full Topic requires one %topic% and at most one %prefix%");
    }
    for (const part of parts) {
      if (part === "%topic%" || part === "%prefix%") continue;
      validateMqttTopic(part);
      if (part.includes("%")) throw new Error("Unsupported Full Topic token: " + part);
    }
    return {
      fullTopic, parts,
      prefixed: parts.includes("%prefix%"),
      literals: parts.filter((p) => p !== "%topic%" && p !== "%prefix%").length,
    };
  }

  private match(topic: string): Match {
    const levels = topic.split("/");
    const matches: Match[] = [];
    for (const pattern of this.full_topics) {
      if (levels.length < pattern.parts.length) continue;
      const path: string[] = [];
      let prefix: string | undefined, topicIndex = 0;
      const matched = pattern.parts.every((part, i) => {
        if (part === "%prefix%") { prefix = levels[i]; return ["cmnd", "stat", "tele"].includes(prefix.toLowerCase()); }
        if (part === "%topic%") topicIndex = path.length;
        path.push(levels[i]);
        return part === "%topic%" || part.toLowerCase() === levels[i].toLowerCase();
      });
      if (matched) matches.push({ ...pattern, path, topicIndex, prefix, suffix: levels.slice(pattern.parts.length) });
    }
    matches.sort((a, b) => b.literals - a.literals || Number(b.prefixed) - Number(a.prefixed));
    if (matches.length > 1 && matches[0].literals === matches[1].literals &&
      matches[0].prefixed === matches[1].prefixed) {
      throw new Error("Ambiguous Full Topic patterns for " + topic);
    }
    return matches[0] || {
      fullTopic: "%topic%/", parts: ["%topic%"], prefixed: false, literals: 0,
      path: [levels[0]], topicIndex: 0, suffix: levels.slice(1),
    };
  }

  private async stream(req: any, res: any): Promise<void> {
    let selectors: string[];
    try {
      let body = req.body;
      if (body === undefined) {
        const chunks = [];
        let size = 0;
        for await (const chunk of req) {
          const buffer = Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk);
          size += buffer.length;
          if (size > 65536) throw new Error("Subscription request is too large");
          chunks.push(buffer);
        }
        body = Buffer.concat(chunks);
      }
      if (Buffer.isBuffer(body)) body = body.toString("utf8");
      if (typeof body === "string") body = JSON.parse(body);
      const topics = Array.isArray(body) ? body : body?.topics;
      if (!Array.isArray(topics) || !topics.length || topics.length > 256) throw new Error("Expected 1 to 256 subscription paths");
      selectors = [...new Set(topics.map((topic) => {
        const { parts, wildcard } = MqttTopicParser.parse(topic, true);
        return MqttTopicParser.format(parts, wildcard);
      }))];
    } catch (err) {
      if (!req.aborted && !res.destroyed && !res.writableEnded) {
        res.statusCode = 400;
        res.setHeader("Content-Type", "application/json");
        res.end(JSON.stringify({ error: err instanceof Error ? err.message : String(err) }));
      }
      return;
    }
    if (req.aborted || res.destroyed || res.writableEnded) return;
    const subscriptions: Array<() => unknown> = [];
    const queue: string[] = [];
    let closed = false, blocked = false;
    const cleanup = () => {
      if (closed) return;
      closed = true;
      for (const unsubscribe of subscriptions) unsubscribe();
      queue.length = 0;
      req.removeListener("aborted", cleanup);
      for (const event of ["close", "error", "finish"]) res.removeListener(event, cleanup);
      res.removeListener("drain", drain);
    };
    const write = (line: string) => {
      try {
        blocked = !res.write(line);
        if (typeof res.flush === "function") res.flush();
      } catch (_err) { cleanup(); res.destroy(); }
    };
    const drain = () => {
      blocked = false;
      while (!closed && !blocked && queue.length) write(queue.shift()!);
    };
    const send = (topic: string, value: any, deleted: boolean) => {
      if (closed || res.destroyed || res.writableEnded) return;
      const line = JSON.stringify({ topic, value, acked: true, ...(deleted ? { deleted: true } : {}) }) + "\n";
      if (blocked) {
        if (queue.length >= 256) { cleanup(); res.destroy(); }
        else queue.push(line);
      } else write(line);
    };
    req.on("aborted", cleanup);
    for (const event of ["close", "error", "finish"]) res.on(event, cleanup);
    res.on("drain", drain);
    res.statusCode = 200;
    res.setHeader("Content-Type", "application/x-ndjson; charset=utf-8");
    res.setHeader("Cache-Control", "no-cache, no-transform");
    res.setHeader("X-Accel-Buffering", "no");
    for (const selector of selectors) subscriptions.push(this.subscribe(selector, send));
    if (typeof res.flushHeaders === "function") res.flushHeaders();
  }
}

class MqttDbNode {
  // "undefined" marks i.e. a subscriber for an otherwise missing topic
  public value: any;
  public children: Record<string, MqttDbNode>;
  // FullTopic to be used when querying/publishing a value.
  // Usually specified only on the topmost node
  public fullTopic?: string;
  public subscribers: Set<Subscription>;

  public constructor(value?: any) {
    this.value = value;
    this.children = Object.create(null);
    this.subscribers = new Set();
  }

  // Build a node tree from a JSON value.
  public static from(value: any): MqttDbNode {
    const build = (current: any): MqttDbNode => {
      const node = new MqttDbNode(object(current) ? {} : current);
      if (object(current)) {
        for (const [key, child] of Object.entries(current)) put(node.children, key, build(child));
      }
      return node;
    };
    return build(json(value));
  }

  // Resolve a path to existing case-insensitive keys or normalized new keys.
  public static resolve(tree: Record<string, MqttDbNode>, parts: string[]): string[] {
    let children: Record<string, MqttDbNode> | undefined = tree;
    return parts.map((input) => {
      const part = children ? storedKey(children, input) : normalizeKey(input);
      children = children?.[part]?.children;
      return part;
    });
  }

  // Find a node at a path.
  public static at(tree: Record<string, MqttDbNode>, parts: string[]): MqttDbNode | undefined {
    let children: Record<string, MqttDbNode> | undefined = tree;
    let node;
    for (const input of parts) {
      if (!children) return undefined;
      const part = storedKey(children, input);
      if (!own(children, part)) return undefined;
      node = children[part];
      children = node.children;
    }
    return node;
  }

  // Find the deepest Full Topic on a picker path.
  public static findFullTopic(tree: Record<string, MqttDbNode>, parts: string[]) {
    let children: Record<string, MqttDbNode> | undefined = tree;
    let found: { fullTopic: string; index: number } | undefined;
    for (let index = 0; children && index < parts.length; index++) {
      const node: MqttDbNode = children[storedKey(children, parts[index])];
      if (!node) break;
      if (node.fullTopic) found = { fullTopic: node.fullTopic, index };
      children = node.children;
    }
    return found;
  }

  // Return this node's detached JSON value.
  public data(): any {
    if (this.value === undefined) return undefined;
    if (!object(this.value)) return json(this.value);
    return Object.fromEntries(Object.entries(this.children).flatMap(([key, child]) => {
      const value = child.data();
      return value === undefined ? [] : [[key, value]];
    }));
  }

  // Move routes and subscriptions to a replacement node.
  public copyMetadata(target: MqttDbNode, subscriptions: Set<Subscription>, copyRoute = true): boolean {
    let kept = false;
    if (copyRoute && this.fullTopic) {
      target.fullTopic = this.fullTopic;
      kept = true;
    }
    for (const sub of this.subscribers) {
      target.subscribers.add(sub);
      sub.node = target;
      subscriptions.add(sub);
      kept = true;
    }
    for (const [key, child] of Object.entries(this.children)) {
      const targetChild = target.children[key] || new MqttDbNode();
      if (child.copyMetadata(targetChild, subscriptions, copyRoute)) {
        if (!target.children[key]) put(target.children, key, targetChild);
        kept = true;
      }
    }
    return kept;
  }

  // Return persisted node data without runtime subscriptions.
  public toJSON(): any {
    const children = Object.fromEntries(Object.entries(this.children).flatMap(([key, child]) => {
      const stored = child.toJSON();
      return stored === undefined ? [] : [[key, stored]];
    }));
    if (this.value === undefined && !this.fullTopic && !Object.keys(children).length) return undefined;
    return {
      ...(this.fullTopic ? { route: this.fullTopic } : {}),
      ...(this.value === undefined ? {} : { value: this.value }),
      ...(Object.keys(children).length || object(this.value) ? { children } : {}),
    };
  }
}
type Pattern = { fullTopic: string; parts: string[]; prefixed: boolean; literals: number };
type Match = Pattern & { path: string[]; topicIndex: number; prefix?: string; suffix: string[] };
type Subscription = {
  parts: string[];
  wildcard: boolean;
  cb: (topic: string, value: any, deleted: boolean) => void;
  node: MqttDbNode;
};
type TopicParser = {
  format(parts: unknown[], wildcard?: boolean): string;
  parse(path: string, allowWildcard?: boolean): { parts: string[]; wildcard: boolean };
};
const MqttTopicParser: TopicParser = module.require("../resources/mqtt-topic-parser");

const own = (obj: object, key: PropertyKey) => Object.prototype.hasOwnProperty.call(obj, key);
const object = (value: unknown): value is Record<string, any> => {
  if (value === null || typeof value !== "object") return false;
  const prototype = Object.getPrototypeOf(value);
  // Also accept plain JSON objects created in Node-RED Function-node VM contexts.
  return prototype === null || Object.getPrototypeOf(prototype) === null;
};
const put = (obj: object, key: PropertyKey, value: any) => Object.defineProperty(obj, key, {
  value, enumerable: true, configurable: true, writable: true,
});
const normalizeKey = (key: string): string => /[A-Z]/.test(key) && !/[a-z]/.test(key) ?
  key[0].toUpperCase() + key.slice(1).toLowerCase() : key;
const storedKey = (children: Record<string, any>, input: string): string =>
  Object.keys(children).find((key) => key.toLowerCase() === input.toLowerCase()) ?? normalizeKey(input);
const starts = (path: string[], prefix: string[]) => prefix.length <= path.length &&
  prefix.every((part, i) => path[i] === part);

// Validate and copy once at the boundary. Never traverse inherited properties.
function json(value: any, convert = false, seen = new Set<any>(), depth = 0, normalize = false): any {
  if (depth > 100) throw new Error("JSON payload is too deeply nested");
  if (typeof value === "string") {
    if (convert && /^(off|false)$/i.test(value)) return 0;
    if (convert && /^(on|true)$/i.test(value)) return 1;
    return value;
  }
  if (typeof value === "boolean") return convert ? Number(value) : value;
  if (value === null || (typeof value === "number" && Number.isFinite(value))) return value;
  if (!object(value) && !Array.isArray(value)) throw new Error("Payload must be a decoded JSON value");
  if (seen.has(value)) throw new Error("JSON payload must not contain cycles");
  seen.add(value);
  let result;
  if (Array.isArray(value)) {
    result = Array.from(value, (v) => json(v, convert, seen, depth + 1, normalize));
  } else {
    result = {};
    for (const [input, child] of Object.entries(value)) {
      put(result, normalize ? storedKey(result, input) : input, json(child, convert, seen, depth + 1, normalize));
    }
  }
  seen.delete(value);
  return result;
}
function validateMqttTopic(topic: string): void {
  if (!topic || topic.split("/").some((s) => !s) || /[\u0000+#]/.test(topic) ||
    Buffer.byteLength(topic) > 1024 || Buffer.from(topic).toString("utf8") !== topic) {
    throw new Error("Invalid managed MQTT topic: " + topic);
  }
}
function leaves(value: any, path: string[], visit: (path: string[]) => void): void {
  if (object(value) && Object.keys(value).length) {
    for (const [key, child] of Object.entries(value)) leaves(child, [...path, key], visit);
  } else visit(path);
}
function clearChildren(node: MqttDbNode, subscriptions: Set<Subscription>): void {
  for (const [key, child] of Object.entries(node.children)) {
    for (const sub of child.subscribers) subscriptions.add(sub);
    child.value = undefined;
    clearChildren(child, subscriptions);
    if (!child.fullTopic && !child.subscribers.size && !Object.keys(child.children).length) delete node.children[key];
  }
}
function merge(dst: Record<string, MqttDbNode>, input: string, value: any, path: string[],
  changed: Map<string, string[]>, subscriptions: Set<Subscription>): void {
  const key = storedKey(dst, input);
  path = [...path.slice(0, -1), key];
  let node = dst[key];
  if (!node) {
    node = new MqttDbNode();
    put(dst, key, node);
  }
  for (const sub of node.subscribers) subscriptions.add(sub);
  if (object(value)) {
    const wasObject = object(node.value);
    if (!wasObject) {
      clearChildren(node, subscriptions);
      node.value = {};
    }
    if (!Object.keys(value).length && !wasObject) changed.set(MqttTopicParser.format(path), path);
    for (const [child, next] of Object.entries(value)) {
      merge(node.children, child, next, [...path, child], changed, subscriptions);
    }
  } else {
    const previous = node.data();
    if (object(previous)) leaves(previous, path, (p) => changed.set(MqttTopicParser.format(p), p));
    clearChildren(node, subscriptions);
    node.value = value;
    changed.set(MqttTopicParser.format(path), path);
  }
}

module.exports = MqttDb;
