const fs = require('fs');
let s = fs.readFileSync('server.js', 'utf8');
s = s.replace(/const fromNumber = typeof i.fromNumber === 'string' \? i.fromNumber\.trim\(\) : '';\r?\n\s*const channel = typeof i.channel === 'string' \? i.channel\.trim\(\)\.slice\(0, 32\) : '';/, "const fromNumber = typeof i.fromNumber === 'string' ? i.fromNumber.trim() : '';\n    const sender = typeof i.sender === 'string' ? i.sender.trim() : (fromNumber || '');\n    const channel = typeof i.channel === 'string' ? i.channel.trim().slice(0, 32) : '';");
s = s.replace(/fromNumber,\r?\n\s*channel,\r?\n\s*createdAt,/, "fromNumber,\n      sender,\n      channel,\n      createdAt,");
fs.writeFileSync('server.js', s);
console.log('done');
