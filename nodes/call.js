module.exports = function(RED) {
  function StringPickerNode(config) {
    RED.nodes.createNode(this, config);
    this.topic = config.topic;
    this.payload = config.payload;
    this.name = config.name;
    this.attr = config.attr;
    this.noexist = config.noexist;
  }

  RED.nodes.registerType('mqtt-db-call', StringPickerNode);
};
