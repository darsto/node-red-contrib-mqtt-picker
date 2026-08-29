class MqttTree {
  static new(obj = {}) {
    const source = obj && typeof obj === "object" ? obj : {};
    return MqttTree.explode_obj(source);
  }

  static explode_obj(obj, prefix = "") {
    if (!obj || typeof obj !== "object" || Array.isArray(obj)) {
      return [];
    }
    return Object.entries(obj).map(([key, value]) => {
      const id = prefix ? `${prefix}.${key}` : key;
      if (value && typeof value === "object" && !Array.isArray(value)) {
        return { id, children: MqttTree.explode_obj(value, id) };
      }
      return { id, value, children: [] };
    });
  }
}
// For internal use
globalThis.MqttTree = MqttTree;

if (typeof window !== "undefined") {
  const set_topic = (topic) => {
    const prev_topic = document.querySelector("#node-input-topic").value;
    document.querySelector("#node-input-topic").value = topic;
    const name = document.querySelector("#node-input-name");
    if (!name.value || name.value == prev_topic) {
      name.value = topic;
    }
  };

  let last_fetch_time = 0;
  let data = null;

  window.open_mqtt_topic_picker = async (e) => {
    e.preventDefault();
    const now = Date.now();
    const fetch_interval_ms = 1000 * 2;
    if (now > last_fetch_time + fetch_interval_ms) {
      let db_data;
      try {
        db_data = await $.getJSON("mqtt-db/data");
      } catch (e) {
        db_data = {};
      }

      data = MqttTree.new(db_data);
      last_fetch_time = now;
    }

    const dialog = document.createElement("div");
    const tree_div = document.createElement("div");
    tree_div.id = "sp-mqtt-picker";
    tree_div.style =
      "border: none; padding: 0; box-shadow: none; max-width: none; margin: 0;";
    dialog.append(tree_div);

    let selected = null;
    let $dialog = null;
    const tree = new Treeview({
      container: tree_div,
      data,
      nodeNameKey: "id",
      searchEnabled: true,
      initiallyExpanded: false,
      multiSelectEnabled: false,
      onSelectionChange: (selectedArray) => {
        if (selectedArray && selectedArray.length > 0) {
          selected = selectedArray[0].id;
          localStorage.setItem("mqttdb-last-selected", selected);
        } else {
          selected = null;
        }
      },
      onRenderNode: (nodeData, nodeContentWrapperElement) => {
        nodeContentWrapperElement.innerHTML = "";
        const nameEl = document.createElement("span");
        nameEl.className = "name";
        const lastDotPos = nodeData.id.lastIndexOf(".");
        const name = nodeData.id.substring(lastDotPos + 1);
        nameEl.textContent = name;
        const removeBtn = document.createElement("span");
        removeBtn.className = "remove";
        removeBtn.title = "Remove node";
        removeBtn.textContent = " 🗑️";
        removeBtn.onclick = async (e) => {
          e.preventDefault();
          e.stopPropagation();
          if (confirm(`Are you sure you want to remove node ${name}`)) {
            const db_data = await $.ajax({
              url: "mqtt-db/data",
              method: "POST",
              contentType: "application/json",
              data: JSON.stringify({
                action: "remove",
                id: nodeData.id,
              }),
            });
            data = MqttTree.new(db_data);
            last_fetch_time = now;
            $dialog.dialog("close");
            window.open_mqtt_topic_picker(e);
          }
        };
        removeBtn.ondblclick = (e) => {
          e.preventDefault();
          e.stopPropagation();
        };
        nameEl.appendChild(removeBtn);

        nodeContentWrapperElement.appendChild(nameEl);
        const typeEl = document.createElement("span");
        typeEl.className = "type";
        typeEl.textContent = "-";
        nodeContentWrapperElement.appendChild(typeEl);
        const valueEl = document.createElement("span");
        valueEl.className = "value";
        valueEl.textContent = nodeData.value?.toString().trim() || "-";
        nodeContentWrapperElement.appendChild(valueEl);

        nameEl.ondblclick =
          typeEl.ondblclick =
          valueEl.ondblclick =
            () => {
              set_topic(nodeData.id);
              $dialog.dialog("close");
            };
      },
    });

    const selected_id = localStorage.getItem("mqttdb-last-selected");
    if (selected_id) {
      tree.selectNodeById(selected_id, true);
      let node = tree.treeviewContainer.querySelector(
        `[data-id="${selected_id}"]`,
      )?.parentElement;
      while (node && node?.id != "sp-mqtt-picker") {
        const expander = node.firstElementChild?.firstElementChild;
        if (expander?.className == "treeview-expander") {
          expander.click();
        }
        node = node.parentElement;
      }
    }

    $dialog = $(dialog).dialog({
      title: "MQTT topic picker",
      modal: true,
      minWidth: 300,
      width: Math.min(1020, Math.round(window.innerWidth * 0.9)),
      minHeight: 400,
      height: Math.round(window.innerHeight * 0.9),
      buttons: {
        Ok: () => {
          if (selected) {
            set_topic(selected);
          }
          $dialog.dialog("close");
        },
        Cancel: () => {
          $dialog.dialog("close");
        },
      },
      close: () => {
        dialog.remove();
      },
    });
  };
}
