"use strict";
// Shared by the runtime and picker: slash is literal; dot separates JSON keys.
class MqttTopicParser {
    // Format JSON key parts as an escaped MQTT picker path.
    static format(parts, wildcard = false) {
        const path = parts.map((part) => String(part).replace(/[\\.#]/g, "\\$&")).join(".");
        return wildcard ? (path ? path + ".#" : "#") : path;
    }
    // Parse an escaped MQTT picker path and optional terminal wildcard.
    static parse(path, allowWildcard = false) {
        if (typeof path !== "string" || !path.length)
            throw new Error("A device path is required");
        const parts = [];
        let part = "";
        let hash = false;
        for (let i = 0; i < path.length; i++) {
            const char = path[i];
            if (char === "\\") {
                const next = path[++i];
                if (!next || !"\\.#".includes(next))
                    throw new Error("Invalid path escape");
                part += next;
            }
            else if (char === ".") {
                if (hash)
                    throw new Error("# is only allowed as the final subscription component");
                parts.push(part);
                part = "";
            }
            else {
                part += char;
                if (char === "#")
                    hash = true;
            }
        }
        const wildcard = hash && part === "#" && allowWildcard;
        if (hash && !wildcard)
            throw new Error("Escape a literal #; only subscriptions accept a final .#");
        if (!wildcard)
            parts.push(part);
        if ((!parts.length && !wildcard) || (parts.length && !parts[0]))
            throw new Error("A device name is required");
        return { parts, wildcard };
    }
}
if (typeof module !== "undefined" && module.exports)
    module.exports = MqttTopicParser;
else
    globalThis.MqttTopicParser = MqttTopicParser;
