module.exports = function(RED) {
  function StringPickerNode(config) {
    RED.nodes.createNode(this, config);
    this.topic = config.topic;
    this.name = config.name;
    this.create = config.create;
  }

  RED.nodes.registerType('mqtt-db-out', StringPickerNode);
};
