# node-red-contrib-mqtt-picker

MQTT device cache and property picker for Node-RED. One JSON file stores the
latest merged device values and their Full Topic patterns. Requires Node.js 18+.

![Topic picker button](mqtt-picker1.jpg)

![Topic picker](mqtt-picker2.jpg)

There are no runtime dependencies or bundled MQTT client. Connect **one**
`mqttdb subscriber` to your existing MQTT input and output nodes. Supply decoded
JSON values: objects, arrays, strings, numbers, booleans, or null. Decode JSON
text upstream when needed; Buffers and other non-JSON objects are rejected.

## Full Topics

Configure one pattern per line in the subscriber's **Full Topics** field.
The default is `%prefix%/%topic%/`. For example:

```text
%prefix%/%topic%/
tasmota/%topic%/%prefix%/
%prefix%/home/cellar/%topic%/
sensors/%topic%/
```

`%topic%` captures one device-name level. Optional `%prefix%` captures
`cmnd`, `stat`, or `tele` at its configured position. Patterns end in `/`;
other levels are literal. More literal levels take precedence; unresolved
ties and conflicting device names are errors. Generic clients use a pattern
without `%prefix%`. Patterns are configured explicitly, not inferred from INFO.

For prefix-bearing devices, RESULT objects merge into the device root. Sole
INFO/STATUS wrappers are unwrapped. Received values `off`/`false` become `0`
and `on`/`true` become `1` in any case, recursively through objects and arrays.
JSON booleans also become 0/1. Other strings, property names, and generic-device
values stay unchanged. Command echoes and `tasmota/discovery/#` are ignored.

## Reading and subscribing

| Selection | `mqttdb in` output |
| --- | --- |
| `desk` | Whole updated device object, once per incoming update |
| `desk.SENSOR` | Whole updated SENSOR object |
| `desk.POWER` | Updated POWER value |
| `#` | Each individual updated leaf across all topics |
| `desk.#` | Each individual updated leaf, with its own path |
| `desk.SENSOR.#` | Each updated leaf below SENSOR |

Updates recursively merge objects; omitted fields remain. Arrays replace as
whole values. Equal values still emit updates. Manual deletion is silent.
Incoming object-to-scalar replacement emits `deleted:true` for removed leaf
paths, with `payload:null`; actual null updates do not have that flag.

`mqttdb call` normally returns cached values without publishing. Its optional
**Query device** mode sends an empty command only for prefix-bearing
`device.suffix` selections and waits for a matching update (default 5000 ms).
Nested and generic queries are unsupported; only one query per device can run.
A response can be delayed/retained or unrelated to the query, so this mode does
not guarantee a freshly sampled measurement. Timeouts and unsupported queries
reach the Node-RED error/Catch mechanism.

The picker and runtime share escaped dotted paths. Escape literal dots,
backslashes, and `#` with a backslash. Slashes are literal: `fan.state/speed`
is a wire suffix, while `fan.state.speed` is a field inside JSON on `state`.
Arrays have no per-index subscriptions. Managed MQTT topics must have nonempty
levels; unsupported topic forms can use ordinary MQTT nodes directly.

## Publishing

`mqttdb out` publishes once and never writes its payload into cached state.
Select/type `desk.POWER` and send `"ON"` to publish to `cmnd/desk/POWER` under
the default pattern, or `tasmota/desk/cmnd/POWER` under the second example.
Outgoing payloads are preserved; normalization applies to received/cached state.

For a generic fan under `devices/%topic%/`, select `fan.set` and send
`{"speed":2}` to publish one JSON message to `devices/fan/set`. Nested JSON
selections such as `fan.state.speed` are rejected as command destinations.
A generic device-only selection publishes an object to its device base.

A command suffix does not need to exist in the cache. An unknown device needs
the output node's optional **Full Topic** setting; it creates routing with empty
values. Conflicting routes are rejected. The old Auto create and Ack filters
are removed: state subscriptions observe received state only.

## Existing data and flows

The file format is now `{ "version": 2, "devices": { ... } }`. Writes use a
temporary file and rename. An old file is backed up as `mqttdb.json.legacy` and
its device objects imported without prefix stripping, with unassigned routes.
An incoming matching message establishes a route, or supply Full Topic on an
output node explicitly. Unassigned devices cannot publish or actively query.
Malformed files are preserved and reported as errors.

Update old parent subscriptions to `device.#` if they need separate leaf
messages. Plain `device` now returns the whole object. Cached prefix-bearing
ON/OFF values are numeric, and renaming topics breaks old references. Obsolete
legacy branches can be removed manually in the picker.

![MQTT backbone](mqtt-picker3.jpg)

## Test run

To start a node-red instance on 127.0.0.1:1880:

```sh
npm install
npm run dev
```

## HTTP API

The MQTT database is available as a read-only runtime endpoint at
`/mqtt-db/data`, relative to `httpNodeRoot`. This endpoint is separate from the
editor endpoint with the same path. It uses Node-RED's standard `httpNodeAuth`
authentication.

Configure separate admin and runtime roots and HTTP node authentication in the
Node-RED `settings.js`:

```js
httpAdminRoot: "/admin",
httpNodeRoot: "/api",
httpNodeAuth: {
  user: "mqtt-site",
  pass: "<bcrypt hash from node-red admin hash-pw>",
},
```

Generate the password hash with Node-RED:

```sh
node-red admin hash-pw
```

The endpoint is then available at `/api/mqtt-db/data`:

```sh
curl --user mqtt-site:password \
  https://node-red.example.com/api/mqtt-db/data
```

Subscribe to multiple topics with `POST /mqtt-db/subscribe`. The response stays
open and streams each matching update as newline-delimited JSON (NDJSON):

```sh
curl -N --user mqtt-site:password \
  -H 'Content-Type: application/json' \
  --data '{"topics":["device.SENSOR","device.POWER"]}' \
  https://node-red.example.com/api/mqtt-db/subscribe
```

The HTTP stream uses the same selection semantics as `mqttdb in`. For the
request above, example lines are:

```json
{"topic":"device.POWER","value":1,"acked":true}
{"topic":"device.SENSOR","value":{"Temperature":21.5},"acked":true}
```

The request body may also be a JSON array of topic strings. Duplicate topics
are subscribed only once. The subscriptions are removed when the HTTP client
disconnects.

`acked:true` is retained for HTTP compatibility and means received state, not
broker acknowledgement. Streams deliver future updates without replay and
disconnect if a slow reader exceeds the bounded pending queue. A final `.#`
selects leaves instead of whole objects. Manual removal sends no stream event.

`httpNodeAuth` protects every endpoint below `httpNodeRoot`, not just this one.
Use HTTPS. For a public website, keep the credentials on the website backend and
proxy authenticated requests to Node-RED.

The editor GET and POST endpoints remain under `httpAdminRoot` and require the
Node-RED permissions `mqtt-db.read` and `mqtt-db.write` when `adminAuth` is
enabled.
