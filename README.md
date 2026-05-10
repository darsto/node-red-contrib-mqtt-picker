# node-red-contrib-mqtt-picker

MQTT nodes for Node-RED with a topic picker in the UI.
All MQTT messages are effectively retained in a local JSON file.

![Topic picker button](mqtt-picker1.jpg)

![Topic picker](mqtt-picker2.jpg)

This is also tailored towards tasmota MQTT states. Specifically,
the topic picker shows stat, tele, and cmnd topics into one device.

There's no MQTT library bundled. In fact, there's zero dependencies.
MQTT messages need to be fed in/out from external sources into
a special mqttdb subscriber node.

![MQTT backbone](mqtt-picker3.jpg)

## Test run

To start a node-red instance on 127.0.0.1:1880:

```sh
npm install
npm run dev
```
