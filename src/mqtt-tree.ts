class MqttTree {
  // Build picker nodes from a value tree.
  public static new(obj: Record<string, any> | null = {}): MqttTreeNode[] {
    let nextId = 0;
    const walk = (value: any, parts: string[] = []): MqttTreeNode[] => {
      if (!value || typeof value !== "object" || Array.isArray(value)) return [];
      return Object.entries(value).map(([label, child]) => {
        const keys = [...parts, label];
        const isObject = child !== null && typeof child === "object" && !Array.isArray(child);
        return {
          id: "mqtt-" + nextId++,
          path: keys.map((key) => String(key).replace(/[\\.#]/g, "\\$&")).join("."),
          label,
          isObject,
          type: child === null ? "null" : Array.isArray(child) ? "array" : typeof child,
          value: isObject ? undefined : child,
          selectable: true,
          children: isObject ? walk(child, keys) : [],
        };
      });
    };
    return walk(obj);
  }

  // Find a picker node by path.
  public static find(nodes: MqttTreeNode[], path: string | null): MqttTreeNode | undefined {
    for (const node of nodes) {
      if (node.path === path) return node;
      const found = MqttTree.find(node.children, path);
      if (found) return found;
    }
  }

  // Format a picker node value for display.
  public static display(node: MqttTreeNode): string {
    return node.isObject ? (node.children.length ? "{…}" : "{}") : JSON.stringify(node.value);
  }
}

declare const $: any;
declare const RED: any;
declare const Treeview: any;

type MqttTreeNode = {
  id: string;
  path: string;
  label: string;
  isObject: boolean;
  type: string;
  value: any;
  selectable: boolean;
  children: MqttTreeNode[];
};

if (typeof module !== "undefined" && module.exports) module.exports = MqttTree;

if (typeof window !== "undefined") {
  let lastFetch = 0;
  let values: Record<string, any> | null = null;
  let dialogState: { width: number; height: number; position: any } | null = null;

  (window as any).open_mqtt_topic_picker = async (e: Event, options: { subscribe?: boolean } = {}) => {
    e.preventDefault();
    if (values === null || Date.now() > lastFetch + 2000) {
      try {
        values = await $.getJSON("mqtt-db/data");
        lastFetch = Date.now();
      } catch (_err) {
        RED.notify("Could not fetch MQTT data. Check the connection and permissions.", "error");
        return;
      }
    }
    const data = MqttTree.new(values);
    const dialog = document.createElement("div");
    const treeDiv = document.createElement("div");
    treeDiv.id = "sp-mqtt-picker";
    treeDiv.style = "border:none;padding:0;box-shadow:none;max-width:none;margin:0";
    dialog.append(treeDiv);
    let selected: MqttTreeNode | null = null;
    let $dialog: any;
    const field = document.querySelector("#node-input-topic") as HTMLInputElement;
    const choose = (path: string) => {
      field.value = path;
      $dialog.dialog("close");
    };
    const tree = new Treeview({
      container: treeDiv,
      data,
      nodeNameKey: "path",
      searchEnabled: true,
      initiallyExpanded: false,
      multiSelectEnabled: false,
      onSelectionChange: (nodes: MqttTreeNode[]) => {
        selected = nodes[0] || null;
        if (selected) localStorage.setItem("mqttdb-last-selected", selected.path);
      },
      onRenderNode: (node: MqttTreeNode, wrapper: HTMLElement) => {
        const name = document.createElement("span");
        name.className = "name";
        name.textContent = node.label;
        const remove = document.createElement("span");
        remove.className = "remove";
        remove.title = "Forget locally (does not publish or notify subscribers)";
        remove.textContent = " 🗑️";
        remove.onclick = async (event) => {
          event.preventDefault();
          event.stopPropagation();
          if (!confirm("Remove " + node.path + " from the local database?")) return;
          try {
            values = await $.ajax({
              url: "mqtt-db/data", method: "POST", contentType: "application/json",
              data: JSON.stringify({ action: "remove", id: node.path }),
            });
            lastFetch = Date.now();
            $dialog.dialog("close");
            (window as any).open_mqtt_topic_picker(event, options);
          } catch (_err) {
            RED.notify("Could not remove MQTT data.", "error");
          }
        };
        remove.ondblclick = (event) => { event.preventDefault(); event.stopPropagation(); };
        name.append(remove);
        const type = document.createElement("span");
        type.className = "type";
        type.textContent = node.type;
        const value = document.createElement("span");
        value.className = "value";
        value.textContent = MqttTree.display(node);
        for (const el of [name, type, value]) {
          el.ondblclick = () => { if (node.selectable) choose(node.path); };
          wrapper.append(el);
        }
      },
    });
    const currentPath = field.value.endsWith(".#") ? field.value.slice(0, -2) : field.value;
    const previous = MqttTree.find(data, currentPath || localStorage.getItem("mqttdb-last-selected"));
    if (previous?.selectable) {
      tree.selectNodeById(previous.id, true);
      // IDs are generated integers, never topic/path text used as CSS.
      let li = treeDiv.querySelector('[data-id="' + previous.id + '"]');
      while (li) {
        if (li.classList.contains("has-children")) {
          li.classList.add("expanded");
          const ul = Array.from(li.children).find((el) => el.tagName === "UL");
          if (ul) (ul as HTMLElement).style.height = "auto";
        }
        li = li.parentElement?.closest("li") ?? null;
      }
    }
    const buttons: Record<string, () => void> = {
      OK: () => { if (selected) choose(selected.path); },
    };
    if (options.subscribe) {
      buttons["Each property (#)"] = () => {
        if (selected?.isObject) choose(selected.path + ".#");
        else RED.notify("Select a device or object first.", "warning");
      };
    }
    buttons.Cancel = () => $dialog.dialog("close");
    const dialogOptions: Record<string, any> = {
      title: "MQTT device properties",
      modal: true, minWidth: 300,
      width: dialogState?.width ?? 800,
      minHeight: 400, height: dialogState?.height ?? 600,
      buttons,
      close: () => {
        dialogState = {
          width: $dialog.dialog("option", "width"),
          height: $dialog.dialog("option", "height"),
          position: $dialog.dialog("option", "position"),
        };
        dialog.remove();
      },
    };
    if (dialogState) dialogOptions.position = dialogState.position;
    $dialog = $(dialog).dialog(dialogOptions);
    const buttonPane = $dialog.dialog("widget").find(".ui-dialog-buttonpane");
    const buttonSet = buttonPane.find(".ui-dialog-buttonset");
    const cancel = buttonSet.children().last();
    const ok = buttonSet.children().first();
    cancel.prependTo(buttonPane).css("margin-right", "auto");
    ok.appendTo(buttonSet);
    buttonPane.css("display", "flex");
  };
}
