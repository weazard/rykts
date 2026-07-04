// DOM-free torrent engine. Extracted from app.js so it can run in any context
// with fetch + IndexedDB + WebCrypto — a page OR a Service Worker. The browser
// is the long-lived process; the Vercel functions are disposable workers for
// the one thing a browser can't do (raw TCP/UDP to peers).
//
// Beyond the original pool, this adds RANGE-DRIVEN scheduling: a media player
// asks for byte ranges, we translate them into a prioritized piece window and
// download those first (sequential + readahead), so playback can start before
// the whole file exists. Cache hits never touch the network — frugal by design.

import { sha1Hex } from "./torrent.js";
import { Store, emptyBitfield, bitGet, bitSet, countSet } from "./store.js";
import { FrameReader } from "./frames.js";

// Worker batch sizing (unchanged tuning from the original orchestrator).
const MIN_BATCH = 8;
const START_BATCH = 16;
const MAX_BATCH = 128;
const FAST_ROUND_MS = 38000;

const MIN_PEERS = 8;
const NUM_WANT = 80;
const MAX_PEER_CACHE = 200;
const ANNOUNCE_COOLDOWN_MS = 5000;
const ANNOUNCE_MAX_BACKOFF_MS = 120000;
const MAX_WORKERS = 4;
const SCALE_UP_GAIN = 1.2;

// Readahead window ahead of the current playback offset. Downloading a bounded
// window (not the whole file) keeps function usage proportional to what's
// actually being watched.
const DEFAULT_READAHEAD_BYTES = 64 * 1024 * 1024;

const noop = () => {};

export class TorrentEngine {
  // opts: { onLog, onProgress, onStats, readaheadBytes }
  constructor(store, opts = {}) {
    this.store = store;
    this.onLog = opts.onLog || noop;
    this.onProgress = opts.onProgress || noop;
    this.onStats = opts.onStats || noop;
    this.readaheadBytes = opts.readaheadBytes || DEFAULT_READAHEAD_BYTES;

    this.meta = null;
    this.session = null;
    this.running = false;

    // worker pool
    this.claimed = new Set();
    this.targetWorkers = 1;
    this.activeWorkers = 0;
    this.nextWorkerId = 0;
    this.autoScale = true;
    this.lastScaleCheck = { atBytes: 0, atTime: 0, throughput: 0 };
    this.bytesDown = 0;
    this.sessionStart = 0;

    // announce discipline
    this.announceInFlight = null;
    this.nextAnnounceAt = 0;
    this.announceBackoffMs = ANNOUNCE_COOLDOWN_MS;
    this.totalWipeoutRounds = 0;

    // Range-driven priority: ordered piece indices the player is waiting on,
    // highest priority first. The pool drains these before sequential fill.
    this.priority = [];
    this.prioritySet = new Set();

    // Piece arrival waiters: index -> array of resolve fns. Resolved the moment
    // a verified piece lands in the store, so readRange can stream progressively.
    this.waiters = new Map();

    this.autoDriveFill = false; // when true, the pool downloads the whole file
  }

  static async open(opts) {
    return new TorrentEngine(await Store.open(), opts);
  }

  log(msg) {
    this.onLog(msg);
  }

  // --- session lifecycle ---

  async loadMeta(meta, seedPeers) {
    this.meta = meta;
    let s = await this.store.getSession(meta.infoHash);
    if (!s) {
      s = {
        infoHash: meta.infoHash,
        meta,
        bitfield: emptyBitfield(meta.pieces.length),
        peers: [],
        peerId: randomPeerIdHex(),
      };
    } else {
      s.meta = meta;
      // Old sessions may predate a field; keep bitfield length in sync.
      if (!s.bitfield || s.bitfield.length !== emptyBitfield(meta.pieces.length).length) {
        s.bitfield = s.bitfield || emptyBitfield(meta.pieces.length);
      }
    }
    this.session = s;
    if (seedPeers?.length) this.mergePeers(seedPeers);
    await this.store.putSession(this.session);
    this.renderProgress();
    return this.session;
  }

  hasPiece(i) {
    return bitGet(this.session.bitfield, i);
  }

  progress() {
    const have = countSet(this.session.bitfield, this.meta.pieces.length);
    return { have, total: this.meta.pieces.length };
  }

  renderProgress() {
    const p = this.progress();
    this.onProgress(p);
  }

  // --- piece accounting ---

  missingIndices() {
    const out = [];
    for (let i = 0; i < this.meta.pieces.length; i++) {
      if (!bitGet(this.session.bitfield, i)) out.push(i);
    }
    return out;
  }

  // Claim up to `n` missing, unclaimed pieces for one worker round. Priority
  // pieces (the player's readahead window) are claimed first and in order; the
  // rest fill sequentially so a paused download still trends toward complete.
  claimBatch(n) {
    const batch = [];
    // 1) priority window, in priority order
    for (const i of this.priority) {
      if (batch.length >= n) break;
      if (bitGet(this.session.bitfield, i) || this.claimed.has(i)) continue;
      this.claimed.add(i);
      batch.push({ index: i, hash: this.meta.pieces[i] });
    }
    // 2) sequential fill (only when driving a full download)
    if (this.autoDriveFill) {
      for (let i = 0; i < this.meta.pieces.length && batch.length < n; i++) {
        if (bitGet(this.session.bitfield, i) || this.claimed.has(i)) continue;
        this.claimed.add(i);
        batch.push({ index: i, hash: this.meta.pieces[i] });
      }
    }
    return batch;
  }

  releaseBatch(batch) {
    for (const w of batch) this.claimed.delete(w.index);
  }

  // --- peers ---

  mergePeers(newPeers) {
    const seen = new Set(this.session.peers.map((p) => `${p.ip}:${p.port}`));
    let added = 0;
    for (const p of newPeers) {
      const k = `${p.ip}:${p.port}`;
      if (seen.has(k)) continue;
      if (this.session.peers.length >= MAX_PEER_CACHE) break;
      this.session.peers.push(p);
      seen.add(k);
      added++;
    }
    return added;
  }

  async ensurePeers() {
    if (this.session.peers.length >= MIN_PEERS) return;
    if (this.announceInFlight) return this.announceInFlight;
    if (Date.now() < this.nextAnnounceAt) return;

    this.announceInFlight = (async () => {
      const have = countSet(this.session.bitfield, this.meta.pieces.length);
      const left = this.meta.totalLength - have * this.meta.pieceLength;
      const res = await postJson("/api/announce", {
        infoHash: this.meta.infoHash,
        announce: this.meta.announce,
        peerId: this.session.peerId,
        left: Math.max(0, left),
        numWant: NUM_WANT,
      });
      if (res.error) {
        this.log("announce error: " + res.error);
        this.announceBackoffMs = Math.min(this.announceBackoffMs * 2, ANNOUNCE_MAX_BACKOFF_MS);
        this.nextAnnounceAt = Date.now() + this.announceBackoffMs;
        return;
      }
      const added = this.mergePeers(res.peers);
      await this.store.putSession(this.session);
      const dht = res.dht?.ok
        ? `DHT ${res.dht.peerCount} peer(s) from ${res.dht.nodesQueried} node(s)`
        : `DHT failed (${res.dht?.error ?? "?"})`;
      this.log(`announce → +${added} peers (cache ${this.session.peers.length}); ${dht}`);
      this.announceBackoffMs =
        added > 0 ? ANNOUNCE_COOLDOWN_MS : Math.min(this.announceBackoffMs * 2, ANNOUNCE_MAX_BACKOFF_MS);
      this.nextAnnounceAt = Date.now() + this.announceBackoffMs;
    })().finally(() => {
      this.announceInFlight = null;
    });
    return this.announceInFlight;
  }

  peersForWorker(workerSlot, totalWorkers) {
    const peers = this.session.peers;
    if (peers.length < totalWorkers * 4) return peers.slice(0, 40);
    const mine = [];
    for (let i = 0; i < peers.length; i++) {
      if (i % totalWorkers === workerSlot % totalWorkers) mine.push(peers[i]);
    }
    return mine.slice(0, 40);
  }

  prunePeers(health) {
    if (!health || health.length === 0) return;
    const failed = health.filter((h) => h.error && h.piecesServed === 0);
    if (failed.length === health.length) {
      this.totalWipeoutRounds++;
      if (this.totalWipeoutRounds === 2) {
        this.log(
          "diagnosis: every peer connection failed with zero data — this " +
            "environment appears to block BitTorrent traffic (DPI). Peers kept.",
        );
      }
      return;
    }
    this.totalWipeoutRounds = 0;
    const bad = new Set(failed.map((h) => `${h.peer.ip}:${h.peer.port}`));
    if (bad.size === 0) return;
    this.session.peers = this.session.peers.filter((p) => !bad.has(`${p.ip}:${p.port}`));
  }

  // --- Range-driven priority ---

  // Translate a file byte range into piece indices, add a readahead window, and
  // set them as the pool's priority. Returns the piece indices covering exactly
  // the requested range (not the readahead) so callers can await them.
  setPriorityForRange(file, start, endInclusive) {
    const pieceLen = this.meta.pieceLength;
    const absStart = file.offset + start;
    const absEnd = file.offset + endInclusive;
    const firstPiece = Math.floor(absStart / pieceLen);
    const lastPiece = Math.floor(absEnd / pieceLen);

    const readaheadPieces = Math.ceil(this.readaheadBytes / pieceLen);
    const windowEnd = Math.min(this.meta.pieces.length - 1, lastPiece + readaheadPieces);

    const ordered = [];
    const set = new Set();
    for (let i = firstPiece; i <= windowEnd; i++) {
      ordered.push(i);
      set.add(i);
    }
    this.priority = ordered;
    this.prioritySet = set;

    const needed = [];
    for (let i = firstPiece; i <= lastPiece; i++) needed.push(i);
    return needed;
  }

  // Promise that resolves once piece `index` is verified & stored.
  waitForPiece(index) {
    if (bitGet(this.session.bitfield, index)) return Promise.resolve();
    return new Promise((resolve) => {
      let arr = this.waiters.get(index);
      if (!arr) {
        arr = [];
        this.waiters.set(index, arr);
      }
      arr.push(resolve);
    });
  }

  notifyPiece(index) {
    const arr = this.waiters.get(index);
    if (arr) {
      this.waiters.delete(index);
      for (const r of arr) r();
    }
  }

  // --- download pool ---

  // Start the worker pool. mode "full" drives the whole file to completion;
  // mode "demand" only downloads priority pieces (streaming) and idles once the
  // readahead window is satisfied — the frugal default for playback.
  async start({ mode = "full", concurrency = "auto" } = {}) {
    if (this.running || !this.session) return;
    this.running = true;
    this.autoDriveFill = mode === "full";

    this.claimed = new Set();
    this.bytesDown = 0;
    this.sessionStart = Date.now();
    this.nextWorkerId = 0;
    this.activeWorkers = 0;
    this.autoScale = concurrency === "auto";
    this.targetWorkers = this.autoScale
      ? 2
      : Math.max(1, Math.min(MAX_WORKERS, Number(concurrency) || 1));
    this.lastScaleCheck = { atBytes: 0, atTime: Date.now(), throughput: 0 };

    await this.ensurePeers();

    const done = [];
    const spawnUpToTarget = () => {
      while (this.activeWorkers < this.targetWorkers && this.running) {
        const id = this.nextWorkerId++;
        this.activeWorkers++;
        done.push(this.worker(id, spawnUpToTarget).finally(() => this.activeWorkers--));
      }
    };
    spawnUpToTarget();
    while (done.length > 0) await Promise.all(done.splice(0));

    this.running = false;
    this.onStats(this.stats());
  }

  stop() {
    this.running = false;
  }

  stats() {
    const dt = this.sessionStart ? (Date.now() - this.sessionStart) / 1000 : 0;
    return {
      bytesDown: this.bytesDown,
      speed: dt > 0 ? this.bytesDown / dt : 0,
      activeWorkers: this.activeWorkers,
      peers: this.session ? this.session.peers.length : 0,
    };
  }

  async worker(id, respawn) {
    let batchSize = START_BATCH;
    let idleRounds = 0;

    while (this.running) {
      if (this.activeWorkers > this.targetWorkers) break;

      let batch = this.claimBatch(batchSize);

      // Web seeds first (BEP 19): plain HTTPS fetches straight from the
      // browser — no peers, no announce, no Vercel function invocations.
      // Whatever the seeds serve is removed from the batch; only the remainder
      // (if any) costs a peer round.
      if (batch.length > 0 && this.meta.webSeeds?.length) {
        const served = await this.webSeedRound(batch);
        if (served.size > 0) {
          const rest = batch.filter((w) => !served.has(w.index));
          this.releaseBatch(batch.filter((w) => served.has(w.index)));
          batch = rest;
          idleRounds = 0;
        }
        if (batch.length === 0) {
          this.onStats(this.stats());
          continue;
        }
      }

      if (batch.length > 0) await this.ensurePeers();

      if (batch.length === 0) {
        // Nothing to claim. In demand mode this is the common idle state: the
        // readahead window is satisfied, so park briefly instead of exiting —
        // a new seek will refill `priority`. In full mode it means we're done.
        if (this.autoDriveFill) break;
        if (this.missingIndices().length === 0) break;
        await sleep(500);
        continue;
      }

      const myPeers = this.peersForWorker(id, Math.max(1, this.activeWorkers));
      if (myPeers.length === 0) {
        this.releaseBatch(batch);
        await sleep(1000);
        continue;
      }

      const t0 = Date.now();
      const round = await this.streamRound({
        infoHash: this.meta.infoHash,
        pieceLength: this.meta.pieceLength,
        totalLength: this.meta.totalLength,
        wanted: batch,
        peers: myPeers,
        peerId: this.session.peerId,
      });
      this.releaseBatch(batch);
      const elapsed = Date.now() - t0;

      if (round.error) {
        this.log(`w${id}: round error: ${round.error}`);
        idleRounds++;
      } else {
        if (round.summary) {
          this.prunePeers(round.summary.peerHealth);
          const pex = round.summary.discoveredPeers ?? [];
          if (pex.length) this.mergePeers(pex);
        }
        await this.store.putSession(this.session);
        if (round.stored === batch.length && elapsed < FAST_ROUND_MS) {
          batchSize = Math.min(MAX_BATCH, batchSize * 2);
        } else if (round.stored < batch.length / 2) {
          batchSize = Math.max(MIN_BATCH, Math.floor(batchSize / 2));
        }
        idleRounds = round.stored === 0 ? idleRounds + 1 : 0;
      }

      if (idleRounds > 0) {
        const wait = Math.min(2000 * 2 ** (idleRounds - 1), 30000);
        await sleep(wait);
      }
      if (idleRounds >= 3 && this.totalWipeoutRounds === 0) {
        this.session.peers = [];
        await this.store.putSession(this.session);
        idleRounds = 0;
      }

      this.maybeRescale(respawn);
      this.onStats(this.stats());
    }
  }

  maybeRescale(respawn) {
    if (!this.autoScale || !this.running) return;
    const now = Date.now();
    const dt = (now - this.lastScaleCheck.atTime) / 1000;
    if (dt < 20) return;
    const throughput = (this.bytesDown - this.lastScaleCheck.atBytes) / dt;
    const prev = this.lastScaleCheck.throughput;
    this.lastScaleCheck = { atBytes: this.bytesDown, atTime: now, throughput };
    if (prev === 0) return;
    if (throughput > prev * SCALE_UP_GAIN && this.targetWorkers < MAX_WORKERS) {
      this.targetWorkers++;
      respawn();
    } else if (throughput < prev * 0.8 && this.targetWorkers > 1) {
      this.targetWorkers--;
    }
  }

  // --- web seeds (BEP 19) ---

  // Try to fetch each piece in `batch` from the torrent's web seeds. Returns
  // the Set of piece indices successfully stored. Seeds that fail (CORS, 404,
  // no Range support) are dropped for the rest of the session.
  async webSeedRound(batch) {
    const served = new Set();
    const CONCURRENCY = 4;
    let cursor = 0;
    const lift = async () => {
      while (cursor < batch.length && this.running !== false) {
        const w = batch[cursor++];
        try {
          const data = await this.fetchPieceFromWebSeeds(w.index);
          if (!data) continue;
          const got = await sha1Hex(data);
          if (got !== this.meta.pieces[w.index]) {
            this.log(`webseed: piece ${w.index} failed verification; discarding`);
            continue;
          }
          if (!bitGet(this.session.bitfield, w.index)) {
            await this.store.putPiece(this.meta.infoHash, w.index, data);
            bitSet(this.session.bitfield, w.index);
            this.bytesDown += data.length;
            this.notifyPiece(w.index);
            this.renderProgress();
          }
          served.add(w.index);
        } catch {
          /* piece falls through to the peer round */
        }
      }
    };
    await Promise.all(Array.from({ length: Math.min(CONCURRENCY, batch.length) }, lift));
    if (served.size > 0) {
      await this.store.putSession(this.session);
      this.log(`webseed: +${served.size} piece(s) over HTTPS (no function calls)`);
    }
    return served;
  }

  // Assemble one piece from web seeds. A piece spans [i*pieceLen, end) in the
  // torrent's concatenated byte space and may straddle file boundaries; per
  // BEP 19, seed URLs ending in "/" get the file path appended.
  async fetchPieceFromWebSeeds(index) {
    if (!this.deadSeeds) this.deadSeeds = new Set();
    const pieceLen = this.meta.pieceLength;
    const absStart = index * pieceLen;
    const absEnd = Math.min(this.meta.totalLength, absStart + pieceLen); // exclusive
    const out = new Uint8Array(absEnd - absStart);

    // File spans overlapped by this piece.
    const spans = [];
    for (const f of this.meta.files) {
      const fStart = f.offset;
      const fEnd = f.offset + f.length;
      const s = Math.max(absStart, fStart);
      const e = Math.min(absEnd, fEnd);
      if (s < e) spans.push({ file: f, from: s - fStart, to: e - fStart, at: s - absStart });
    }

    for (const span of spans) {
      let ok = false;
      for (const seed of this.meta.webSeeds) {
        if (this.deadSeeds.has(seed)) continue;
        const url = webSeedFileUrl(seed, this.meta, span.file);
        try {
          const res = await fetch(url, {
            headers: { range: `bytes=${span.from}-${span.to - 1}` },
          });
          if (res.status !== 206 && res.status !== 200) throw new Error("HTTP " + res.status);
          const buf = new Uint8Array(await res.arrayBuffer());
          // A 200 means the server ignored Range; slice what we need.
          const bytes = res.status === 200 ? buf.subarray(span.from, span.to) : buf;
          if (bytes.length !== span.to - span.from) throw new Error("short read");
          out.set(bytes, span.at);
          ok = true;
          break;
        } catch (e) {
          this.deadSeeds.add(seed);
          this.log(`webseed ${seed} disabled: ${String(e?.message ?? e)}`);
        }
      }
      if (!ok) return null;
    }
    return out;
  }

  async streamRound(body) {
    let stored = 0;
    let bytes = 0;
    let summary = null;
    try {
      const res = await fetch(new URL("/api/download", selfOrigin()), {
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
            const got = await sha1Hex(frame.data);
            if (got !== this.meta.pieces[frame.index]) {
              this.log(`piece ${frame.index} failed verification; discarding`);
              continue;
            }
            if (!bitGet(this.session.bitfield, frame.index)) {
              await this.store.putPiece(this.meta.infoHash, frame.index, frame.data);
              bitSet(this.session.bitfield, frame.index);
              stored++;
              bytes += frame.data.length;
              this.bytesDown += frame.data.length;
              this.notifyPiece(frame.index);
              this.renderProgress();
            }
          } else if (frame.type === "summary") {
            summary = frame.summary;
          }
        }
      }
      return { stored, bytes, summary };
    } catch (e) {
      return { error: String(e?.message ?? e), stored, bytes, summary };
    }
  }

  // --- range reads (for the streaming server shim) ---

  // Ensure the pool is running in demand mode (idempotent). Used by readRange so
  // a media request kicks off downloading without a manual start().
  ensureDemandPool() {
    if (!this.running) {
      // Fire and forget; the pool self-parks when the window is satisfied.
      this.start({ mode: "demand", concurrency: "auto" }).catch((e) =>
        this.log("demand pool error: " + String(e?.message ?? e)),
      );
    }
  }

  // Read [start, endInclusive] bytes of a file as a ReadableStream, downloading
  // any missing pieces (priority-first) as needed. Bytes already cached stream
  // out immediately; missing pieces block only until they arrive.
  readRange(file, start, endInclusive) {
    const needed = this.setPriorityForRange(file, start, endInclusive);
    this.ensureDemandPool();
    const engine = this;
    const pieceLen = this.meta.pieceLength;
    let abs = file.offset + start;
    const absEnd = file.offset + endInclusive;
    let pieceCursor = 0;

    return new ReadableStream({
      async pull(controller) {
        if (abs > absEnd) {
          controller.close();
          return;
        }
        const index = needed[pieceCursor];
        // Re-prioritize on each pull: a fresh seek replaces `priority`, which
        // would otherwise starve this still-open read forever.
        if (!engine.hasPiece(index) && !engine.prioritySet.has(index)) {
          engine.priority.unshift(index);
          engine.prioritySet.add(index);
        }
        engine.ensureDemandPool();
        await engine.waitForPiece(index);
        const blob = await engine.store.getPieceBlob(engine.meta.infoHash, index);
        if (!blob) {
          controller.error(new Error("piece vanished from store: " + index));
          return;
        }
        const pieceStartAbs = index * pieceLen;
        const from = Math.max(0, abs - pieceStartAbs);
        const to = Math.min(blob.size, absEnd - pieceStartAbs + 1);
        const slice = blob.slice(from, to);
        const bytes = new Uint8Array(await slice.arrayBuffer());
        controller.enqueue(bytes);
        abs = pieceStartAbs + to;
        pieceCursor++;
        if (pieceCursor >= needed.length || abs > absEnd) controller.close();
      },
    });
  }

  // --- storage lifecycle passthroughs ---

  getPieceBlob(index) {
    return this.store.getPieceBlob(this.meta.infoHash, index);
  }

  async clearTorrent() {
    const n = this.meta?.pieces?.length ?? this.session?.meta?.pieces?.length ?? 0;
    await this.store.clearTorrent(this.session.infoHash, n);
    this.session.bitfield = emptyBitfield(n);
    this.session.peers = [];
    this.renderProgress();
  }
}

// --- helpers (self-contained so this module runs in a SW too) ---

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

// BEP 19 URL mapping: seed URLs ending in "/" are directories — append the
// file path (which already starts with the torrent name for multi-file
// torrents). Other URLs point directly at the single file.
function webSeedFileUrl(seed, meta, file) {
  if (!seed.endsWith("/")) return seed;
  return seed + file.path.split("/").map(encodeURIComponent).join("/");
}

// location.href in a page, self.location in a worker/SW.
function selfOrigin() {
  return typeof location !== "undefined" ? location.href : self.location.href;
}

function randomPeerIdHex() {
  const id = new Uint8Array(20);
  const prefix = "-SL0001-";
  for (let i = 0; i < prefix.length; i++) id[i] = prefix.charCodeAt(i);
  crypto.getRandomValues(id.subarray(prefix.length));
  let s = "";
  for (let i = 0; i < id.length; i++) s += id[i].toString(16).padStart(2, "0");
  return s;
}

export async function postJson(url, body) {
  try {
    const res = await fetch(new URL(url, selfOrigin()), {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify(body),
    });
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
