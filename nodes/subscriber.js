
module.exports = function(RED) {
    function MqttDbSubscriber(config) {
        RED.nodes.createNode(this, config);
        this.db = null;

        this.connect = (node) => {
            if (!this.db) {
                this.db = {};
            }
        }

        this.subscribe = (topic, cb) => {

        }

        this.on('close', function() {
            if (this.db) {
                this.db = null;
            }
        });
    }
    RED.nodes.registerType("mqtt-db-subscriber", MqttDbSubscriber);
}
