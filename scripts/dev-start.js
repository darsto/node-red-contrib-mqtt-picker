const { spawn } = require('child_process');
const path = require('path');
const fs = require('fs');

async function run() {
  const root = path.resolve(__dirname, '..');
  const dataDir = path.join(root, 'data');

  // ensure data dir exists
  if (!fs.existsSync(dataDir)) fs.mkdirSync(dataDir);

  console.log('Installing local package into data/ ...');
  const npmInstall = spawn('npm', ['install'], { cwd: dataDir, stdio: 'inherit' });
  npmInstall.on('close', (code) => {
    if (code !== 0) {
      console.error('npm install failed in data/ with code', code);
      process.exit(code);
    }
    console.log('Starting Node-RED (userDir=data)...');
    const nr = spawn('node-red', ['-u', dataDir], { stdio: 'inherit' });
    nr.on('close', (c) => process.exit(c));
  });
}
run();
