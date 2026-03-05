const fs = require('fs');

let serverTxt = fs.readFileSync('server.js', 'utf8');

const target = serverTxt.substring(serverTxt.indexOf('const matched = matchProjectFromText(store, body);'), serverTxt.indexOf('businessKey: routing.businessKey,'));

const replacement = `const matched = matchProjectFromText(store, body);

    const senderKey = normalizePhoneForLookup(from);
    let finalProjectName = matched?.name || '';
    let fromLabel = from || '';

    const pMap = store.settings?.senderProjectMap || settings.senderProjectMap || {};
    if (pMap[senderKey]) {
      fromLabel = pMap[senderKey].projectName;
      if (!finalProjectName) {
          finalProjectName = fromLabel;
          if (matched) { matched.id = pMap[senderKey].projectId; }
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
      projectId: matched?.id || '',
      projectName: finalProjectName,
      `;

serverTxt = serverTxt.replace(target, replacement);

fs.writeFileSync('server.js', serverTxt, 'utf8');
console.log('Done!');