module.exports = function(RED) {
    function MqttDbSubscriber(n) {
        RED.nodes.createNode(this,n);
        this.command = n.command;

        this.on('close', function() {
            console.log("mqtt-db-subscriber close");
        });
    }
    RED.nodes.registerType("mqtt-db-subscriber", MqttDbSubscriber);
}
