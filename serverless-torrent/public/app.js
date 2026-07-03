// Orchestrator: drives a pool of bounded /api/download invocations, persisting
// every verified piece to IndexedDB as it streams in. The browser is the
// long-lived process; the functions are disposable workers. The loop survives
// page reloads because all state (bitfield, peers, pieces) is in the Store.

import {
  parseTorrent,
  sha1Hex,
  randomPeerIdHex,
  parseMagnet,
  metaFromInfoDict,
  bytesFromBase64,
} from "./torrent.js";
import { Store, emptyBitfield, bitGet, bitSet, countSet } from "./store.js";
import { FrameReader } from "./frames.js";

// Per-worker batch sizing. A worker that drains its batch well before the
// server's ~50s budget doubles its next batch; one that can't finish shrinks.
const MIN_BATCH = 8;
const START_BATCH = 16;
const MAX_BATCH = 128;
// Server self-limits to ~50s; a round that finished this fast had spare budget.
const FAST_ROUND_MS = 38000;

const MIN_PEERS = 8; // re-announce when the cache drops below this
const NUM_WANT = 80;
const MAX_PEER_CACHE = 200;
const MAX_WORKERS = 4;
// In auto mode: scale up only if throughput improved at least this much since
// the previous worker count was in effect.
const SCALE_UP_GAIN = 1.2;

let store;
let meta = null; // parsed torrent metadata
let session = null; // { infoHash, meta, bitfield, peers, peerId }
let running = false;

// --- worker pool state (reset per start()) ---
let claimed = new Set(); // piece indices currently assigned to some worker
let targetWorkers = 1;
let activeWorkers = 0;
let nextWorkerId = 0;
let autoScale = true;
let lastScaleCheck = { atBytes: 0, atTime: 0, throughput: 0 };
let bytesDown = 0;
let sessionStart = 0;

const $ = (id) => document.getElementById(id);
const log = (msg) => {
  const el = $("log");
  el.textContent += `[${new Date().toLocaleTimeString()}] ${msg}\n`;
  el.scrollTop = el.scrollHeight;
};

async function init() {
  store = await Store.open();
  $("file").addEventListener("change", onFile);
  $("loadMagnet").addEventListener("click", onMagnet);
  $("magnet").addEventListener("keydown", (e) => {
    if (e.key === "Enter" && !e.nativeEvent?.isComposing && e.keyCode !== 229) onMagnet();
  });
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

  // If we already fetched metadata for this infohash, reuse it — sessions are
  // durable, so magnets resume instantly across reloads.
  const cached = await store.getSession(m.infoHash);
  if (cached?.meta?.pieces?.length) {
    log(`magnet: reusing cached metadata for ${m.infoHash}`);
    await loadMeta(cached.meta, []);
    return;
  }

  log(`magnet: fetching metadata for ${m.infoHash} (DHT + ${m.announce.length} tracker(s))…`);
  $("loadMagnet").disabled = true;
  try {
    const res = await postJson("/api/metadata", {
      infoHash: m.infoHash,
      announce: m.announce,
    });
    if (res.error) {
      log("metadata error: " + res.error);
      return;
    }
    const parsed = await metaFromInfoDict(bytesFromBase64(res.infoBase64), m.announce);
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
  meta = parsedMeta;

  let s = await store.getSession(meta.infoHash);
  if (!s) {
    s = {
      infoHash: meta.infoHash,
      meta,
      bitfield: emptyBitfield(meta.pieces.length),
      peers: [],
      peerId: randomPeerIdHex(),
    };
  } else {
    s.meta = meta; // refresh in case file re-picked
  }
  session = s;
  if (seedPeers?.length) mergePeers(seedPeers);
  await store.putSession(session);

  $("name").textContent = meta.name;
  $("details").textContent =
    `${meta.pieces.length} pieces × ${meta.pieceLength} B · ${(meta.totalLength / 1e6).toFixed(2)} MB · ` +
    `${meta.files.length} file(s) · ${meta.announce.length} tracker(s)`;
  $("start").disabled = false;
  renderProgress();
  log(`loaded ${meta.name} (infohash ${meta.infoHash})`);
}

// --- piece accounting ---

function missingIndices() {
  const out = [];
  for (let i = 0; i < meta.pieces.length; i++) {
    if (!bitGet(session.bitfield, i)) out.push(i);
  }
  return out;
}

// Claim up to `n` unassigned missing pieces for one worker round. Claims keep
// the workers' piece ranges disjoint, so parallel invocations never duplicate
// each other's work.
function claimBatch(n) {
  const batch = [];
  for (let i = 0; i < meta.pieces.length && batch.length < n; i++) {
    if (bitGet(session.bitfield, i)) continue;
    if (claimed.has(i)) continue;
    claimed.add(i);
    batch.push({ index: i, hash: meta.pieces[i] });
  }
  return batch;
}

function releaseBatch(batch) {
  for (const w of batch) claimed.delete(w.index);
}

// --- peer management ---

function mergePeers(newPeers) {
  const seen = new Set(session.peers.map((p) => `${p.ip}:${p.port}`));
  let added = 0;
  for (const p of newPeers) {
    const k = `${p.ip}:${p.port}`;
    if (seen.has(k)) continue;
    if (session.peers.length >= MAX_PEER_CACHE) break;
    session.peers.push(p);
    seen.add(k);
    added++;
  }
  return added;
}

async function ensurePeers() {
  if (session.peers.length >= MIN_PEERS) return;
  log(
    `peer cache low (${session.peers.length}); announcing ` +
      `(${meta.announce.length} tracker(s) + DHT)…`,
  );
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
  const added = mergePeers(res.peers);
  await store.putSession(session);
  const dht = res.dht?.ok
    ? `DHT ${res.dht.peerCount} peer(s) from ${res.dht.nodesQueried} node(s)`
    : `DHT failed (${res.dht?.error ?? "?"})`;
  log(`announce → +${added} new peers (cache ${session.peers.length}); ${dht}`);
}

// Stripe the peer cache across workers so parallel invocations talk to mostly
// disjoint peers (each peer's upload slots aren't split N ways). With few
// peers, everyone shares the whole list.
function peersForWorker(workerSlot, totalWorkers) {
  const peers = session.peers;
  if (peers.length < totalWorkers * 4) return peers.slice(0, 40);
  const mine = [];
  for (let i = 0; i < peers.length; i++) {
    if (i % totalWorkers === workerSlot % totalWorkers) mine.push(peers[i]);
  }
  return mine.slice(0, 40);
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

// --- download pool ---

async function start() {
  if (running || !session) return;
  running = true;
  $("start").disabled = true;
  $("stop").disabled = false;

  claimed = new Set();
  bytesDown = 0;
  sessionStart = Date.now();
  nextWorkerId = 0;
  activeWorkers = 0;

  const sel = $("concurrency").value;
  autoScale = sel === "auto";
  targetWorkers = autoScale ? 2 : Math.max(1, Math.min(MAX_WORKERS, Number(sel) || 1));
  lastScaleCheck = { atBytes: 0, atTime: Date.now(), throughput: 0 };
  log(`starting download pool (workers: ${autoScale ? "auto, starting at 2" : targetWorkers})`);

  await ensurePeers();
  if (session.peers.length === 0 && missingIndices().length > 0) {
    log("no peers available; pausing loop");
    running = false;
    $("start").disabled = false;
    $("stop").disabled = true;
    return;
  }

  const done = [];
  const spawnUpToTarget = () => {
    while (activeWorkers < targetWorkers && running) {
      const id = nextWorkerId++;
      activeWorkers++;
      done.push(
        worker(id, spawnUpToTarget).finally(() => {
          activeWorkers--;
        }),
      );
    }
  };
  spawnUpToTarget();
  // Auto-scaling can push new worker promises into `done` while we wait, and
  // Promise.all snapshots its argument — so drain in waves until quiet.
  while (done.length > 0) {
    await Promise.all(done.splice(0));
  }

  if (missingIndices().length === 0) {
    log("all pieces present — assembling files");
    await assemble();
  }

  running = false;
  $("start").disabled = false;
  $("stop").disabled = true;
  renderStats();
}

// One worker = a loop of bounded /api/download rounds over disjoint batches.
// `respawn` lets a finishing round trigger pool growth in auto mode.
async function worker(id, respawn) {
  let batchSize = START_BATCH;
  let idleRounds = 0;

  while (running) {
    // Pool shrank below the current size: whichever worker hits this boundary
    // first exits (one per round boundary until the pool matches the target).
    if (activeWorkers > targetWorkers) break;

    await ensurePeers();
    const batch = claimBatch(batchSize);
    if (batch.length === 0) break; // nothing left unclaimed — pool is finishing

    const myPeers = peersForWorker(id, Math.max(1, activeWorkers));
    if (myPeers.length === 0) {
      releaseBatch(batch);
      break;
    }

    log(`w${id}: requesting ${batch.length} pieces from ${myPeers.length} peers…`);
    const t0 = Date.now();
    const round = await streamRound({
      infoHash: meta.infoHash,
      pieceLength: meta.pieceLength,
      totalLength: meta.totalLength,
      wanted: batch,
      peers: myPeers,
      peerId: session.peerId,
    });
    releaseBatch(batch);
    const elapsed = Date.now() - t0;

    if (round.error) {
      log(`w${id}: round error: ${round.error}`);
      idleRounds++;
    } else {
      if (round.summary) {
        prunePeers(round.summary.peerHealth);
        const pex = round.summary.discoveredPeers ?? [];
        if (pex.length) {
          const added = mergePeers(pex);
          if (added) log(`w${id}: PEX discovered ${added} new peer(s)`);
        }
      } else {
        log(`w${id}: stream truncated; kept ${round.stored} verified piece(s)`);
      }
      await store.putSession(session);

      log(
        `w${id}: +${round.stored}/${batch.length} pieces (${(round.bytes / 1e6).toFixed(1)} MB, ` +
          `${elapsed} ms${round.summary?.hitDeadline ? ", hit deadline" : ""})`,
      );

      // Adaptive batch: drained everything with spare budget → go bigger;
      // finished under half → go smaller.
      if (round.stored === batch.length && elapsed < FAST_ROUND_MS) {
        batchSize = Math.min(MAX_BATCH, batchSize * 2);
      } else if (round.stored < batch.length / 2) {
        batchSize = Math.max(MIN_BATCH, Math.floor(batchSize / 2));
      }
      idleRounds = round.stored === 0 ? idleRounds + 1 : 0;
    }

    if (idleRounds >= 3) {
      log(`w${id}: no progress for 3 rounds; refreshing peer cache`);
      session.peers = []; // force re-announce (tracker + DHT)
      await store.putSession(session);
      idleRounds = 0;
    }

    maybeRescale(respawn);
    renderStats();
  }
}

// Auto mode: grow the pool while throughput keeps improving, shrink when a
// larger pool didn't pay for itself. Checked at round boundaries only.
function maybeRescale(respawn) {
  if (!autoScale || !running) return;
  const now = Date.now();
  const dt = (now - lastScaleCheck.atTime) / 1000;
  if (dt < 20) return; // let the current size prove itself first

  const throughput = (bytesDown - lastScaleCheck.atBytes) / dt; // bytes/sec
  const prev = lastScaleCheck.throughput;
  lastScaleCheck = { atBytes: bytesDown, atTime: now, throughput };

  if (prev === 0) return; // first sample, nothing to compare against
  if (throughput > prev * SCALE_UP_GAIN && targetWorkers < MAX_WORKERS) {
    targetWorkers++;
    log(`pool: throughput up (${fmtSpeed(throughput)}); scaling to ${targetWorkers} workers`);
    respawn();
  } else if (throughput < prev * 0.8 && targetWorkers > 1) {
    targetWorkers--;
    log(`pool: throughput down (${fmtSpeed(throughput)}); scaling to ${targetWorkers} workers`);
  }
}

// One streamed /api/download round. Pieces are verified and persisted the
// moment their frame arrives — progress is live, memory stays flat.
async function streamRound(body) {
  let stored = 0;
  let bytes = 0;
  let summary = null;
  try {
    const res = await fetch(new URL("/api/download", location.href), {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify(body),
    });
    if (!res.ok || !res.body) {
      const text = await res.text().catch(() => "");
      let detail;
      try {
        detail = JSON.parse(text).error;
      } catch {
        detail = text.trim().split("\n")[0].slice(0, 200) || res.statusText;
      }
      return { error: `HTTP ${res.status}: ${detail}`, stored, bytes, summary };
    }

    const reader = res.body.getReader();
    const frames = new FrameReader();
    for (;;) {
      const { done, value } = await reader.read();
      if (done) break;
      frames.push(value);
      for (const frame of frames.frames()) {
        if (frame.type === "piece") {
          // Client-side verification is the source of truth — corruption can
          // never be committed to durable storage.
          const got = await sha1Hex(frame.data);
          if (got !== meta.pieces[frame.index]) {
            log(`piece ${frame.index} failed verification; discarding`);
            continue;
          }
          if (!bitGet(session.bitfield, frame.index)) {
            await store.putPiece(meta.infoHash, frame.index, frame.data);
            bitSet(session.bitfield, frame.index);
            stored++;
            bytes += frame.data.length;
            bytesDown += frame.data.length;
            renderProgress();
          }
        } else if (frame.type === "summary") {
          summary = frame.summary;
        }
      }
    }
    return { stored, bytes, summary };
  } catch (e) {
    // Keep whatever was stored before the failure; those pieces are durable.
    return { error: String(e?.message ?? e), stored, bytes, summary };
  }
}

// --- rendering ---

function renderProgress() {
  const have = countSet(session.bitfield, meta.pieces.length);
  const pct = ((have / meta.pieces.length) * 100).toFixed(1);
  $("bar").style.width = pct + "%";
  $("pct").textContent = `${have}/${meta.pieces.length} pieces (${pct}%)`;
}

function renderStats() {
  if (!sessionStart) return;
  const dt = (Date.now() - sessionStart) / 1000;
  const speed = dt > 0 ? bytesDown / dt : 0;
  $("stats").textContent =
    `${(bytesDown / 1e6).toFixed(1)} MB this session · avg ${fmtSpeed(speed)} · ` +
    `${activeWorkers} worker(s) active · ${session.peers.length} peers cached`;
}

function fmtSpeed(bps) {
  if (bps > 1e6) return (bps / 1e6).toFixed(2) + " MB/s";
  return (bps / 1e3).toFixed(0) + " kB/s";
}

// --- assembly (Blob-backed: never materializes the whole file in memory) ---

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
  // Blob.slice() and new Blob([blobs...]) are lazy — the browser stitches disk
  // -backed references instead of copying bytes, so multi-GB files assemble
  // without loading pieces into memory.
  const parts = [];
  let remaining = file.length;
  let abs = file.offset;
  while (remaining > 0) {
    const index = Math.floor(abs / meta.pieceLength);
    const within = abs % meta.pieceLength;
    const piece = await store.getPieceBlob(meta.infoHash, index);
    if (!piece) throw new Error("missing piece " + index + " during assembly");
    const take = Math.min(piece.size - within, remaining);
    parts.push(piece.slice(within, within + take));
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
    // The endpoint may return a non-JSON body on failure (e.g. a platform 500
    // page like "A server error has occurred"). Read as text and parse defensively
    // so a crashed function surfaces a clear error instead of an opaque
    // "Unexpected token … is not valid JSON".
    const text = await res.text();
    try {
      return JSON.parse(text);
    } catch {
      const detail = text.trim().split("\n")[0].slice(0, 200) || res.statusText;
      return { error: `server returned HTTP ${res.status}: ${detail}` };
    }
  } catch (e) {
    return { error: String(e?.message ?? e) };
  }
}

init();
