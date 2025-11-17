module.exports = function(RED) {
  function StringPickerNode(config) {
    RED.nodes.createNode(this, config);
    this.topic = config.topic;
    this.payload = config.payload;
    this.name = config.name;
    this.attr = config.attr;
    this.ack = config.ack;
  }

  RED.nodes.registerType('mqtt-db-in', StringPickerNode);
};
