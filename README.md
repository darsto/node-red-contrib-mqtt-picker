# node-red-contrib-string-picker

This is a small Node-RED node that exposes a text input with a "Pick" button in the node edit dialog. Clicking the button opens a small dialog with a list of hard-coded string options; picking one fills the text field.

Developer quickstart

1. Install dependencies (in project root):

   npm install

2. Start the dev environment (this will install the local package into a test userDir and start Node-RED):

   npm run dev

3. Open the Node-RED editor at http://127.0.0.1:1880 and add the "string-picker" node from the palette.

Notes
- The dev script installs the current package into `./data` (so Node-RED will load it) and starts Node-RED using `data` as the user directory.
