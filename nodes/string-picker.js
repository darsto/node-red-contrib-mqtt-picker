module.exports = function(RED) {
  function StringPickerNode(config) {
    RED.nodes.createNode(this, config);
    var node = this;
    node.name = config.name;
    node.picked = config.picked || '';

    node.on('input', function(msg, send, done) {
      // Simple behavior: set payload to the configured string (or existing msg.payload if empty)
      msg.payload = node.picked || msg.payload;
      send(msg);
      if (done) { done(); }
    });
  }
  RED.nodes.registerType('string-picker', StringPickerNode);
};
