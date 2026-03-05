const fs = require("fs");
let s = fs.readFileSync("server.js", "utf8");

s = s.replace(
  /const ts = nowIso\(\);\s*const nextItem = normalizeInboxItem\(\{[\s\S]*?updatedAt: ts,\s*\}\);/g,
  (match, p1) => {
    if (match.includes("id,") && match.includes("source: cleanSource,")) {
      return `const ts = nowIso();

      let finalProjectId = projectId;
      let finalProjectName = projectName;
      const senderKey = fromNumber || "";
      if (!finalProjectId && senderKey && store.senderProjectMap && store.senderProjectMap[senderKey]) {
        const autoProjId = store.senderProjectMap[senderKey];
        const autoProj = (store.projects || []).find(p => p.id === autoProjId);
        if (autoProj) {
          finalProjectId = autoProj.id;
          finalProjectName = autoProj.name;
        }
      }

      const nextItem = normalizeInboxItem({
        id,
        source: cleanSource,
        text: cleanText,
        status: "New",
        projectId: finalProjectId,
        projectName: finalProjectName,
        businessKey,
        businessLabel,
        toNumber,
        fromNumber,
        sender: senderKey,
        channel,
        createdAt: ts,
        updatedAt: ts,
      });`;
    }
    return match;
  }
);
fs.writeFileSync("server.js", s);
console.log("done fix5");
