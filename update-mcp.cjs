const fs = require('fs');
const p = require('path');
const f = p.join(require('os').homedir(), 'AppData', 'Roaming', 'Task Tracker', 'settings.json');
const s = JSON.parse(fs.readFileSync(f, 'utf8'));

// Ensure mcpServers exists
if (!Array.isArray(s.mcpServers)) {
    s.mcpServers = [];
}

// Check if puppeteer is already in there
const hasPup = s.mcpServers.find(x => x.id === 'puppeteer');
if (!hasPup) {
    s.mcpServers.push({
        id: 'puppeteer',
        name: 'Web Browser',
        enabled: true,
        command: 'npx.cmd',
        args: ['-y', '@modelcontextprotocol/server-puppeteer']
    });
    fs.writeFileSync(f, JSON.stringify(s, null, 2));
    console.log('Added Puppeteer MCP.');
} else {
    console.log('Puppeteer MCP already present.');
}
