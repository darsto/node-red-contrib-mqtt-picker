
module.exports = function(RED) {
  function StringPickerNode(config) {
    RED.nodes.createNode(this, config);
    const node = this;
    node.name = config.name;

    node.on('close', function(removed, done) {
      done();
    });
  }

  RED.nodes.registerType('mqtt-db-in', StringPickerNode);
};
