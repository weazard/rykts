// Thin UI layer over the DOM-free TorrentEngine. This page is the manual
// download/verify harness; the same engine also powers the Stremio streaming
// Service Worker (local-server-sw.js). Keeping one engine means fixes apply to
// both surfaces.

import {
  parseTorrent,
  parseMagnet,
  metaFromInfoDict,
  bytesFromBase64,
} from "./torrent.js";
import { TorrentEngine, postJson } from "./engine.js";
import { requestPersistentStorage, storageEstimate } from "./store.js";

let engine = null;

const $ = (id) => document.getElementById(id);
const log = (msg) => {
  const el = $("log");
  el.textContent += `[${new Date().toLocaleTimeString()}] ${msg}\n`;
  el.scrollTop = el.scrollHeight;
};

// Object URLs created for fallback downloads — tracked so we can revoke them.
const objectUrls = new Set();
function trackedObjectUrl(blob) {
  const url = URL.createObjectURL(blob);
  objectUrls.add(url);
  return url;
}
function revokeObjectUrls() {
  for (const url of objectUrls) URL.revokeObjectURL(url);
  objectUrls.clear();
}

async function init() {
  engine = await TorrentEngine.open({
    onLog: log,
    onProgress: renderProgress,
    onStats: renderStats,
  });
  const persisted = await requestPersistentStorage();
  log(
    persisted
      ? "storage: persistent (won't be auto-evicted)"
      : "storage: best-effort (browser may evict under pressure)",
  );
  $("file").addEventListener("change", onFile);
  $("loadMagnet").addEventListener("click", onMagnet);
  $("magnet").addEventListener("keydown", (e) => {
    if (e.key === "Enter" && !e.nativeEvent?.isComposing && e.keyCode !== 229) onMagnet();
  });
  $("start").addEventListener("click", start);
  $("stop").addEventListener("click", () => {
    engine.stop();
    log("stop requested");
  });
  $("clear").addEventListener("click", clearStoredData);
  await renderStorage();
}

async function renderStorage() {
  const est = await storageEstimate();
  const el = $("storage");
  if (!est) {
    el.textContent = "";
    return;
  }
  const usedMb = est.usage / 1e6;
  const used = usedMb > 1000 ? `${(usedMb / 1000).toFixed(2)} GB` : `${usedMb.toFixed(0)} MB`;
  const quotaGb = est.quota / 1e9;
  el.textContent = `storage in use: ${used} of ~${quotaGb.toFixed(1)} GB available`;
  $("clear").disabled = !engine.session || est.usage === 0;
}

async function onFile(e) {
  const file = e.target.files[0];
  if (!file) return;
  const buf = new Uint8Array(await file.arrayBuffer());
  await loadMeta(await parseTorrent(buf), []);
}

async function onMagnet() {
  const uri = $("magnet").value.trim();
  if (!uri) return;
  let m;
  try {
    m = parseMagnet(uri);
  } catch (e) {
    log("magnet error: " + e.message);
    return;
  }

  const cached = await engine.store.getSession(m.infoHash);
  if (cached?.meta?.pieces?.length) {
    log(`magnet: reusing cached metadata for ${m.infoHash}`);
    await loadMeta(cached.meta, []);
    return;
  }

  $("loadMagnet").disabled = true;
  try {
    // Frugal path first: an "xs" hint is a direct HTTPS URL to the .torrent —
    // no peers or Vercel functions needed for metadata at all.
    if (m.xs) {
      try {
        log(`magnet: fetching .torrent from xs hint (no function calls)…`);
        const buf = new Uint8Array(await (await fetch(m.xs)).arrayBuffer());
        const parsed = await parseTorrent(buf);
        if (parsed.infoHash === m.infoHash) {
          parsed.webSeeds = [...new Set([...(parsed.webSeeds || []), ...(m.webSeeds || [])])];
          await loadMeta(parsed, []);
          return;
        }
        log("magnet: xs hint hash mismatch; falling back to peer metadata");
      } catch (e) {
        log(`magnet: xs fetch failed (${e?.message ?? e}); falling back to peer metadata`);
      }
    }

    log(`magnet: fetching metadata for ${m.infoHash} (DHT + ${m.announce.length} tracker(s))…`);
    const res = await postJson("/api/metadata", { infoHash: m.infoHash, announce: m.announce });
    if (res.error) {
      log("metadata error: " + res.error);
      return;
    }
    const parsed = await metaFromInfoDict(bytesFromBase64(res.infoBase64), m.announce, m.webSeeds);
    if (parsed.infoHash !== m.infoHash) {
      log("metadata error: info dict hash mismatch; discarding");
      return;
    }
    log(`metadata: ${res.name} (${res.pieceCount} pieces) via ${res.peers.length} peer(s)`);
    await loadMeta(parsed, res.peers);
  } finally {
    $("loadMagnet").disabled = false;
  }
}

async function loadMeta(parsedMeta, seedPeers) {
  const meta = parsedMeta;
  await engine.loadMeta(meta, seedPeers);
  $("name").textContent = meta.name;
  $("details").textContent =
    `${meta.pieces.length} pieces × ${meta.pieceLength} B · ${(meta.totalLength / 1e6).toFixed(2)} MB · ` +
    `${meta.files.length} file(s) · ${meta.announce.length} tracker(s)`;
  $("start").disabled = false;
  await renderStorage();
  log(`loaded ${meta.name} (infohash ${meta.infoHash})`);
}

async function start() {
  if (engine.running || !engine.session) return;
  $("start").disabled = true;
  $("stop").disabled = false;
  const sel = $("concurrency").value;
  log(`starting download pool (workers: ${sel === "auto" ? "auto, starting at 2" : sel})`);

  await engine.start({ mode: "full", concurrency: sel });

  if (engine.missingIndices().length === 0) {
    log("all pieces present — assembling files");
    await assemble();
  }
  $("start").disabled = false;
  $("stop").disabled = true;
  renderStats(engine.stats());
  await renderStorage();
}

// --- rendering ---

function renderProgress(p) {
  const prog = p || engine.progress();
  const pct = ((prog.have / prog.total) * 100).toFixed(1);
  $("bar").style.width = pct + "%";
  $("pct").textContent = `${prog.have}/${prog.total} pieces (${pct}%)`;
}

function renderStats(s) {
  const st = s || engine.stats();
  $("stats").textContent =
    `${(st.bytesDown / 1e6).toFixed(1)} MB this session · avg ${fmtSpeed(st.speed)} · ` +
    `${st.activeWorkers} worker(s) active · ${st.peers} peers cached`;
}

function fmtSpeed(bps) {
  if (bps > 1e6) return (bps / 1e6).toFixed(2) + " MB/s";
  return (bps / 1e3).toFixed(0) + " kB/s";
}

// --- assembly & storage lifecycle ---

async function assemble() {
  const meta = engine.meta;
  const links = $("downloads");
  links.innerHTML = "";
  revokeObjectUrls();

  const canStream = typeof window.showSaveFilePicker === "function";
  for (const f of meta.files) {
    const li = document.createElement("li");
    if (canStream) {
      const btn = document.createElement("button");
      btn.textContent = `save ${f.path} (${(f.length / 1e6).toFixed(2)} MB) to disk`;
      btn.addEventListener("click", () => saveFileStreaming(f, btn));
      li.appendChild(btn);
    } else {
      const a = document.createElement("a");
      a.href = trackedObjectUrl(await assembleFile(f));
      a.download = f.path.split("/").pop();
      a.textContent = `download ${f.path} (${(f.length / 1e6).toFixed(2)} MB)`;
      li.appendChild(a);
    }
    links.appendChild(li);
  }
  log(
    canStream
      ? "files ready — saving streams directly to disk, then storage is freed"
      : "files ready — after saving, use 'Clear stored data' to free disk space",
  );
  await renderStorage();
}

async function saveFileStreaming(file, btn) {
  const meta = engine.meta;
  let handle;
  try {
    handle = await window.showSaveFilePicker({ suggestedName: file.path.split("/").pop() });
  } catch {
    return;
  }
  btn.disabled = true;
  try {
    const writable = await handle.createWritable();
    let remaining = file.length;
    let abs = file.offset;
    while (remaining > 0) {
      const index = Math.floor(abs / meta.pieceLength);
      const within = abs % meta.pieceLength;
      const piece = await engine.getPieceBlob(index);
      if (!piece) throw new Error("missing piece " + index + " during save");
      const take = Math.min(piece.size - within, remaining);
      await writable.write(piece.slice(within, within + take));
      abs += take;
      remaining -= take;
    }
    await writable.close();
    log(`saved ${file.path} to disk`);
    if (meta.files.length === 1) {
      await engine.clearTorrent();
      log("freed stored pieces — storage reclaimed");
      renderProgress();
      await renderStorage();
    }
  } catch (e) {
    log("save error: " + (e?.message ?? e));
    btn.disabled = false;
  }
}

async function assembleFile(file) {
  const meta = engine.meta;
  const parts = [];
  let remaining = file.length;
  let abs = file.offset;
  while (remaining > 0) {
    const index = Math.floor(abs / meta.pieceLength);
    const within = abs % meta.pieceLength;
    const piece = await engine.getPieceBlob(index);
    if (!piece) throw new Error("missing piece " + index + " during assembly");
    const take = Math.min(piece.size - within, remaining);
    parts.push(piece.slice(within, within + take));
    abs += take;
    remaining -= take;
  }
  return new Blob(parts);
}

async function clearStoredData() {
  if (!engine.session) return;
  if (engine.running) {
    log("stop the download before clearing stored data");
    return;
  }
  revokeObjectUrls();
  $("downloads").innerHTML = "";
  await engine.clearTorrent();
  log(`cleared all stored data for ${engine.session.infoHash}`);
  renderProgress();
  await renderStorage();
}

init();
