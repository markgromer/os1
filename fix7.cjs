const fs = require("fs");
let s = fs.readFileSync("public/app.js", "utf8");

s = s.replace(
  /<span class="px-2 py-0\.5 rounded border\s*border-zinc-800 bg-zinc-950\/40 text-\[10px\] font-mono\s*text-zinc-300">\$\{escapeHtml\(inboxBusinessLabel\(item\)\)\}<\/span>/g,
  `<span class="px-2 py-0.5 rounded border border-zinc-800 bg-zinc-950/40 text-[10px] font-mono text-zinc-300">\${escapeHtml(inboxBusinessLabel(item))}</span>
                                  \${(item?.sender || item?.fromNumber) ? \`<span class="px-2 py-0.5 rounded border border-zinc-500/30 bg-zinc-800/40 text-[10px] font-mono text-zinc-300">\${escapeHtml(item?.sender || item?.fromNumber)}</span>\` : ""}`
);
fs.writeFileSync("public/app.js", s);
console.log("done ui change")
