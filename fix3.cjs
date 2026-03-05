const fs = require('fs');
let s = fs.readFileSync('server.js', 'utf8');

const search = "      const ts = nowIso();\n      const nextItem = {\n        ...item,\n        status: 'New',\n        createdAt: ts,\n        updatedAt: ts,\n      };";
const replace = "      const ts = nowIso();\n      \n      let finalProjectId = item.projectId;\n      let finalProjectName = item.projectName;\n      const senderKey = item.sender || item.fromNumber || '';\n      if (!finalProjectId && senderKey && store.senderProjectMap && store.senderProjectMap[senderKey]) {\n        const autoProjId = store.senderProjectMap[senderKey];\n        const autoProj = (store.projects || []).find(p => p.id === autoProjId);\n        if (autoProj) {\n          finalProjectId = autoProj.id;\n          finalProjectName = autoProj.name;\n        }\n      }\n\n      const nextItem = {\n        ...item,\n        projectId: finalProjectId,\n        projectName: finalProjectName,\n        status: 'New',\n        createdAt: ts,\n        updatedAt: ts,\n      };";
s = s.replace(search, replace);
fs.writeFileSync('server.js', s);
console.log('done /api/inbox replace!');
