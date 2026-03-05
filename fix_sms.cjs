const fs = require('fs');
let text = fs.readFileSync('server.js', 'utf8');

text = text.replace(
  /const routing = resolveBusinessForInbound\(.*\}\);\n\s*const store = await readStore\(\);\n\s*const matched = matchProjectFromText\(.*\}\);\n\s*const lines = \[\];\n\s*lines\.push\(`SMS\$\{from.*\}\);\n\s*lines\.push\(body\);\n\n\s*await addInboxIntegrationItem\(\{\n\s*source: 'sms',\n\s*externalId: `sms:\$\{sid \|\| crypto\.createHash.*\},\n\s*text: lines\.join\(\'\\n\'\),\n\s*projectId: matched\?\.id \|\| '',\n\s*projectName: matched\?\.name \|\| '',/,
  `const routing = resolveBusinessForInbound({ settings, toNumber: to });
    const store = await readStore();
    let matched = matchProjectFromText(store, body) || {};
    
    // Attempt map
    const senderKey = normalizePhoneForLookup(from);
    let finalProjectName = matched.name || '';
    let fromLabel = from || '';
    
    // Map SMS Sender ID to real name & project 
    const pMap = store.senderProjectMap || store.settings?.senderProjectMap || settings.senderProjectMap || {};
    if (pMap[senderKey]) {
      fromLabel = pMap[senderKey].projectName;
      if (!finalProjectName) {
          finalProjectName = fromLabel;
          matched.id = pMap[senderKey].projectId;
      }
    }

    const lines = [];
    lines.push(\`📱 SMS • \${routing.businessLabel}\`);
    lines.push(\`From: \${fromLabel}\`);
    lines.push(\`To: \${to}\`);
    lines.push(\`\`);
    lines.push(body);

    await addInboxIntegrationItem({
      source: 'sms',
      externalId: \`sms:\${sid || crypto.createHash('sha1').update(\`\${from}|\${to}|\${body}\`).digest('hex')}\`,
      text: lines.join('\\n'),
      projectId: matched.id || '',
      projectName: finalProjectName,`
);

fs.writeFileSync('server.js', text, 'utf8');
console.log('Update complete!');