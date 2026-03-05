const fs = require('fs');
let s = fs.readFileSync('server.js', 'utf8');
const search =       const ts = nowIso();
      const nextItem = {
        ...item,
        status: 'New',
        createdAt: ts,
        updatedAt: ts,
      };;

const replace =       const ts = nowIso();
      
      let finalProjectId = item.projectId;
      let finalProjectName = item.projectName;
      const senderKey = item.sender || item.fromNumber || '';
      if (!finalProjectId && senderKey && store.senderProjectMap && store.senderProjectMap[senderKey]) {
        const autoProjId = store.senderProjectMap[senderKey];
        const autoProj = (store.projects || []).find(p => p.id === autoProjId);
        if (autoProj) {
          finalProjectId = autoProj.id;
          finalProjectName = autoProj.name;
        }
      }

      const nextItem = {
        ...item,
        projectId: finalProjectId,
        projectName: finalProjectName,
        status: 'New',
        createdAt: ts,
        updatedAt: ts,
      };;
s = s.replace(search, replace);
fs.writeFileSync('server.js', s);
console.log('done /api/inbox replace!');
