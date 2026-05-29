// Orchestrator: drives repeated, bounded /api/download calls, persisting every
// verified piece to IndexedDB. The browser is the long-lived process; the
// functions are disposable workers. The loop survives page reloads because all
// state (bitfield, peers, pieces) is in the Store.

import { parseTorrent, sha1Hex, randomPeerIdHex } from "./torrent.js";
import { Store, emptyBitfield, bitGet, bitSet, countSet } from "./store.js";

const BATCH_PIECES = 16; // pieces requested per /api/download call
const MIN_PEERS = 8; // re-announce when the cache drops below this
const NUM_WANT = 80;

let store;
let meta = null; // parsed torrent
let session = null; // { infoHash, meta, bitfield, peers, peerId }
let running = false;

const $ = (id) => document.getElementById(id);
const log = (msg) => {
  const el = $("log");
  el.textContent += `[${new Date().toLocaleTimeString()}] ${msg}\n`;
  el.scrollTop = el.scrollHeight;
};

async function init() {
  store = await Store.open();
  $("file").addEventListener("change", onFile);
  $("start").addEventListener("click", start);
  $("stop").addEventListener("click", () => {
    running = false;
    log("stop requested");
  });
}

async function onFile(e) {
  const file = e.target.files[0];
  if (!file) return;
  const buf = new Uint8Array(await file.arrayBuffer());
  meta = await parseTorrent(buf);

  let s = await store.getSession(meta.infoHash);
  if (!s) {
    s = {
      infoHash: meta.infoHash,
      meta,
      bitfield: emptyBitfield(meta.pieces.length),
      peers: [],
      peerId: randomPeerIdHex(),
    };
    await store.putSession(s);
  } else {
    s.meta = meta; // refresh in case file re-picked
  }
  session = s;

  $("name").textContent = meta.name;
  $("details").textContent =
    `${meta.pieces.length} pieces × ${meta.pieceLength} B · ${(meta.totalLength / 1e6).toFixed(2)} MB · ` +
    `${meta.files.length} file(s) · ${meta.announce.length} tracker(s)`;
  $("start").disabled = false;
  renderProgress();
  log(`loaded ${meta.name} (infohash ${meta.infoHash})`);
}

function missingPieces() {
  const out = [];
  for (let i = 0; i < meta.pieces.length; i++) {
    if (!bitGet(session.bitfield, i)) out.push({ index: i, hash: meta.pieces[i] });
  }
  return out;
}

async function ensurePeers() {
  if (session.peers.length >= MIN_PEERS) return;
  if (!meta.announce.length) {
    log("no trackers in torrent; cannot discover peers");
    return;
  }
  log(`peer cache low (${session.peers.length}); announcing…`);
  const have = countSet(session.bitfield, meta.pieces.length);
  const left = meta.totalLength - have * meta.pieceLength;
  const res = await postJson("/api/announce", {
    infoHash: meta.infoHash,
    announce: meta.announce,
    peerId: session.peerId,
    left: Math.max(0, left),
    numWant: NUM_WANT,
  });
  if (res.error) {
    log("announce error: " + res.error);
    return;
  }
  // merge, dedup
  const seen = new Set(session.peers.map((p) => `${p.ip}:${p.port}`));
  for (const p of res.peers) {
    const k = `${p.ip}:${p.port}`;
    if (!seen.has(k)) {
      session.peers.push(p);
      seen.add(k);
    }
  }
  await store.putSession(session);
  log(`announce → ${res.peers.length} peers (cache now ${session.peers.length})`);
}

async function start() {
  if (running || !session) return;
  running = true;
  $("start").disabled = true;
  $("stop").disabled = false;
  log("starting download loop");

  let idleRounds = 0;
  while (running) {
    const missing = missingPieces();
    if (missing.length === 0) {
      log("all pieces present — assembling files");
      await assemble();
      break;
    }

    await ensurePeers();
    if (session.peers.length === 0) {
      log("no peers available; pausing loop");
      break;
    }

    const batch = missing.slice(0, BATCH_PIECES);
    log(`requesting ${batch.length} pieces from ${session.peers.length} peers…`);
    const res = await postJson("/api/download", {
      infoHash: meta.infoHash,
      pieceLength: meta.pieceLength,
      totalLength: meta.totalLength,
      wanted: batch,
      peers: session.peers,
      peerId: session.peerId,
    });
    if (res.error) {
      log("download error: " + res.error);
      break;
    }

    let stored = 0;
    for (const p of res.pieces) {
      const bytes = base64ToBytes(p.data);
      // Client-side verification is the source of truth — corruption can never
      // be committed to durable storage.
      const got = await sha1Hex(bytes);
      if (got !== meta.pieces[p.index]) {
        log(`piece ${p.index} failed verification; discarding`);
        continue;
      }
      await store.putPiece(meta.infoHash, p.index, bytes);
      bitSet(session.bitfield, p.index);
      stored++;
    }
    prunePeers(res.peerHealth);
    await store.putSession(session);

    log(
      `+${stored}/${batch.length} pieces verified & stored ` +
        `(${res.elapsedMs} ms${res.hitDeadline ? ", hit deadline" : ""})`,
    );
    renderProgress();

    idleRounds = stored === 0 ? idleRounds + 1 : 0;
    if (idleRounds >= 3) {
      log("no progress for 3 rounds; refreshing peers");
      session.peers = []; // force re-announce
      idleRounds = 0;
    }
  }

  running = false;
  $("start").disabled = false;
  $("stop").disabled = true;
}

function prunePeers(health) {
  if (!health) return;
  // Drop peers that connected but errored and served nothing; keep the rest.
  const bad = new Set(
    health.filter((h) => h.error && h.piecesServed === 0).map((h) => `${h.peer.ip}:${h.peer.port}`),
  );
  if (bad.size === 0) return;
  session.peers = session.peers.filter((p) => !bad.has(`${p.ip}:${p.port}`));
}

function renderProgress() {
  const have = countSet(session.bitfield, meta.pieces.length);
  const pct = ((have / meta.pieces.length) * 100).toFixed(1);
  $("bar").style.width = pct + "%";
  $("pct").textContent = `${have}/${meta.pieces.length} pieces (${pct}%)`;
}

async function assemble() {
  const links = $("downloads");
  links.innerHTML = "";
  for (const f of meta.files) {
    const blob = await assembleFile(f);
    const a = document.createElement("a");
    a.href = URL.createObjectURL(blob);
    a.download = f.path.split("/").pop();
    a.textContent = `download ${f.path} (${(f.length / 1e6).toFixed(2)} MB)`;
    const li = document.createElement("li");
    li.appendChild(a);
    links.appendChild(li);
  }
  log("files ready");
}

async function assembleFile(file) {
  const parts = [];
  let remaining = file.length;
  let abs = file.offset;
  while (remaining > 0) {
    const index = Math.floor(abs / meta.pieceLength);
    const within = abs % meta.pieceLength;
    const piece = await store.getPiece(meta.infoHash, index);
    if (!piece) throw new Error("missing piece " + index + " during assembly");
    const take = Math.min(piece.length - within, remaining);
    parts.push(piece.subarray(within, within + take));
    abs += take;
    remaining -= take;
  }
  return new Blob(parts);
}

// --- helpers ---

async function postJson(url, body) {
  try {
    // Resolve to an absolute URL against the page origin. Safari's fetch rejects
    // bare relative paths with "The string did not match the expected pattern.".
    const res = await fetch(new URL(url, location.href), {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify(body),
    });
    return await res.json();
  } catch (e) {
    return { error: String(e?.message ?? e) };
  }
}

function base64ToBytes(b64) {
  const bin = atob(b64);
  const out = new Uint8Array(bin.length);
  for (let i = 0; i < bin.length; i++) out[i] = bin.charCodeAt(i);
  return out;
}

init();
