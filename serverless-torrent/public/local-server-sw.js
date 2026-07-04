// Stremio streaming-server shim, running entirely in a Service Worker.
//
// Stremio's UI is built to talk to a streaming server at a configured address
// (normally http://127.0.0.1:11470/ from stremio-service). Instead of running
// that remote binary, we point Stremio at OUR origin under a /stream/ prefix
// and answer the exact HTTP contract its core (stremio-core) speaks — but the
// bytes come from the in-browser TorrentEngine, which only delegates raw
// peer TCP to disposable Vercel functions. No remote server, no open ports.
//
// Contract surface (paths relative to the server base, i.e. our /stream/):
//   GET  settings                         → SettingsResponse (server probe)
//   GET  casting | network-info | device-info | get-https  → benign stubs
//   POST {infoHashHex}/create             → ensure engine + return torrent info
//   GET  {infoHashHex}/{fileIdx}          → Range streaming (206/HEAD)
//   GET  {infoHashHex}/{fileIdx}/stats.json → Statistics
//   GET  hlsv2/...                        → handled in a later task (501 for now)
//
// fileIdx of -1 means "largest file", per stremio-core.

import { TorrentEngine, webSeedFileUrl } from "./engine.js";
import { metaFromInfoDict, parseTorrent, bytesFromBase64 } from "./torrent.js";
import { handleHls } from "./hls.js";

const PREFIX = "/stream/";
const SERVER_VERSION = "4.20.8"; // version we report; matches a real server shape

// One engine per infohash, kept in SW module scope. If the SW is evicted the
// engines vanish but IndexedDB pieces persist, so a fresh SW rebuilds state.
const engines = new Map(); // infoHashHex -> TorrentEngine
const engineBoot = new Map(); // infoHashHex -> Promise<TorrentEngine> (dedup)

self.addEventListener("install", () => self.skipWaiting());
self.addEventListener("activate", (event) => event.waitUntil(self.clients.claim()));

// stremio-core joins endpoint paths onto the configured base URL, so its
// requests arrive under PREFIX (/stream/...). stremio-video however builds
// URLs with url.resolve(serverURL, '/absolute/path'), which discards the
// /stream/ prefix and lands at the origin root. Since this SW controls scope
// '/', we route those root-level server endpoints too. Every pattern here is
// unambiguous vs. the SPA's assets (40-hex infohash prefix or a reserved
// server path), so normal page/asset requests still pass through untouched.
// A 40-hex first segment alone is NOT enough: stremio-web's build serves its
// own assets under a 40-hex commit-hash directory (/{hash}/scripts/main.js).
// Only claim the path when the second segment is a torrent endpoint:
// create, stats.json, or a (possibly negative) numeric fileIdx.
const HASH_RE = /^\/[0-9a-fA-F]{40}\/(create$|stats\.json$|-?\d+(\/|$))/;
const ROOT_SERVER_RE = /^\/(hlsv2|proxy)(\/|$)|^\/(subtitles\.vtt|opensubHash|casting|network-info|device-info|get-https)$/;

function serverRest(url) {
  if (url.pathname.startsWith(PREFIX)) return url.pathname.slice(PREFIX.length);
  if (HASH_RE.test(url.pathname) || ROOT_SERVER_RE.test(url.pathname)) {
    return url.pathname.slice(1);
  }
  return null;
}

self.addEventListener("fetch", (event) => {
  const url = new URL(event.request.url);
  if (url.origin !== self.location.origin) return;
  const rest = serverRest(url);
  if (rest === null) return;
  event.respondWith(handle(event.request, url, rest));
});

async function handle(request, url, rest) {
  try {
    const segments = rest.split("/").filter(Boolean);

    // --- probe / info endpoints ---
    if (segments.length === 0 || segments[0] === "settings") {
      return json(settingsResponse(url));
    }
    if (segments[0] === "casting") return json([]);
    if (segments[0] === "network-info") {
      return json({ availableInterfaces: ["127.0.0.1"] });
    }
    if (segments[0] === "device-info") {
      return json({ available: [], choices: [] });
    }
    if (segments[0] === "get-https") {
      // We can't provision TLS for a LAN IP; tell the UI it's unavailable.
      return json({ error: "https provisioning not supported by this server" }, 501);
    }
    if (segments[0] === "hlsv2") {
      // Probe + HLS transcoding, delegated to hls.js. Probing sniffs codecs
      // from the file's own bytes (probe.js); segments are transcoded by
      // either the /api/transcode function (web-seeded sources) or
      // ffmpeg.wasm in a controlled page. See hls.js for the contract.
      return await handleHls(request, url, resolveMedia);
    }
    // Video-layer extras. All failures here are non-critical to playback
    // (stremio-video catches them and falls back to nulls).
    if (segments[0] === "opensubHash") {
      // OpenSubtitles moviehash needs the first+last 64KiB; skip until a
      // subtitles task needs it. 200 + error field per server convention.
      return json({ error: "opensubHash not supported", result: null });
    }
    if (segments[0] === "subtitles.vtt") {
      return json({ error: "subtitle conversion not supported" }, 501);
    }
    if (segments[0] === "proxy") {
      return json({ error: "proxying not supported by the in-browser server" }, 501);
    }

    // --- torrent endpoints: {infoHash}/... ---
    const infoHash = normalizeHash(segments[0]);
    if (infoHash) {
      // POST {infoHash}/create
      if (segments[1] === "create" && request.method === "POST") {
        return await handleCreate(request, infoHash);
      }
      // GET {infoHash}/stats.json (torrent-level, no fileIdx)
      if (segments[1] === "stats.json") {
        return await handleStats(infoHash, null, url);
      }
      // GET {infoHash}/{fileIdx}/stats.json
      if (segments.length >= 3 && segments[2] === "stats.json") {
        return await handleStats(infoHash, segments[1], url);
      }
      // GET {infoHash}/{fileIdx}
      if (segments.length >= 2) {
        return await handleStream(request, infoHash, segments[1], url);
      }
    }

    return json({ error: "not found" }, 404);
  } catch (e) {
    return json({ error: String(e?.message ?? e) }, 500);
  }
}

// --- settings (server detection) ---
// Shape mirrors stremio-core's SettingsResponse test fixture so the UI parses
// it and marks the server online.
function settingsResponse(url) {
  const baseUrl = url.origin + PREFIX.replace(/\/$/, "");
  return {
    baseUrl,
    values: {
      serverVersion: SERVER_VERSION,
      appPath: "/browser",
      cacheRoot: "/browser",
      cacheSize: 2147483648,
      btMaxConnections: 55,
      btHandshakeTimeout: 20000,
      btRequestTimeout: 4000,
      btDownloadSpeedSoftLimit: 2621440,
      btDownloadSpeedHardLimit: 3670016,
      btMinPeersForStable: 5,
      remoteHttps: "",
      localAddonEnabled: false,
      transcodeHorsepower: 0.75,
      transcodeMaxBitRate: 0,
      transcodeConcurrency: 1,
      transcodeTrackConcurrency: 1,
      transcodeHardwareAccel: false,
      transcodeProfile: null,
      allTranscodeProfiles: [],
      transcodeMaxWidth: 1920,
    },
    options: [
      {
        id: "cacheSize",
        label: "CACHING",
        type: "select",
        selections: [
          { name: "no caching", val: 0 },
          { name: "2GB", val: 2147483648 },
          { name: "∞", val: null },
        ],
      },
    ],
  };
}

// --- create: ensure the engine exists and hand back the torrent's files ---
async function handleCreate(request, infoHash) {
  let body = {};
  try {
    body = await request.json();
  } catch {
    /* body optional */
  }
  const announce = trackersFromPeerSearch(body);
  const webSeeds = Array.isArray(body?.webSeeds) ? body.webSeeds : [];
  const xs = typeof body?.xs === "string" && /^https?:\/\//i.test(body.xs) ? body.xs : null;
  const engine = await ensureEngine(infoHash, announce, webSeeds, xs);
  const info = torrentInfo(engine);
  // stremio-video's createTorrent sends guessFileIdx ({} or {season,episode})
  // when the stream has no explicit fileIdx and reads `guessedFileIdx` off the
  // response to build the media URL.
  if (body?.guessFileIdx !== false && body?.guessFileIdx !== undefined) {
    info.guessedFileIdx = guessFileIdx(engine.meta.files, body.guessFileIdx || {});
  }
  return json(info);
}

// Pick the file a series/movie stream most likely refers to: prefer files
// whose path matches the requested SxxExx, then the largest video file.
const VIDEO_EXT_RE = /\.(mp4|mkv|webm|avi|mov|m4v|ts|m2ts|wmv|flv)$/i;
function guessFileIdx(files, { season, episode } = {}) {
  let candidates = files.filter((f) => VIDEO_EXT_RE.test(f.path));
  if (candidates.length === 0) candidates = files;
  if (season != null && episode != null) {
    const pad = (n) => String(n).padStart(2, "0");
    const re = new RegExp(
      `s${pad(season)}[ ._-]*e${pad(episode)}|${season}x${pad(episode)}|s${season}e${episode}`,
      "i"
    );
    const hit = candidates.filter((f) => re.test(f.path));
    if (hit.length > 0) candidates = hit;
  }
  const best = candidates.reduce((a, b) => (b.length > a.length ? b : a), candidates[0]);
  return files.indexOf(best);
}

// --- media resolution for hls.js (probe + transcoding) ---
// Turn a mediaURL (one of our own /{infoHash}/{fileIdx}?tr=..&ws=.. stream
// URLs) into { engine, file, webSeedUrl }. webSeedUrl is a direct HTTPS URL
// to the same file when the torrent has a BEP 19 web seed — that's what lets
// the /api/transcode function read the source server-side.
async function resolveMedia(mediaURL) {
  const media = new URL(mediaURL, self.location.origin);
  const segs = media.pathname.replace(/^\/stream\//, "/").split("/").filter(Boolean);
  const infoHash = normalizeHash(segs[0]);
  if (!infoHash || segs.length < 2) {
    throw new Error("unsupported mediaURL (not a torrent stream)");
  }
  const engine = await ensureEngine(
    infoHash,
    trackersFromQuery(media),
    media.searchParams.getAll("ws").filter((u) => /^https?:\/\//i.test(u)),
    null
  );
  const file = pickFile(engine, segs[1]);
  if (!file) throw new Error("no such file");
  const seeds = engine.meta.webSeeds || [];
  const webSeedUrl = seeds.length > 0 ? webSeedFileUrl(seeds[0], engine.meta, file) : null;
  return { engine, file, webSeedUrl };
}

// --- stats.json ---
async function handleStats(infoHash, fileIdxRaw, url) {
  const engine = engines.get(infoHash);
  if (!engine || !engine.meta) {
    return json({ error: "torrent not created" }, 404);
  }
  const file = pickFile(engine, fileIdxRaw);
  const p = engine.progress();
  const s = engine.stats();
  const streamProgress = p.total ? p.have / p.total : 0;
  return json({
    name: engine.meta.name,
    infoHash,
    files: engine.meta.files.map(fileEntry),
    // Index of the file the engine would stream for fileIdx=-1 (largest).
    // The video layer's fetchFilename prefers this over guessing itself.
    guessedFileIdx: engine.meta.files.indexOf(pickFile(engine, null)),
    sources: [],
    opts: {
      dht: true,
      tracker: true,
      path: "/browser",
      connections: engine.session?.peers?.length ?? 0,
      peerSearch: { min: 40, max: 200, sources: [`dht:${infoHash}`] },
      swarmCap: { maxSpeed: null, minPeers: null },
      growler: { flood: 0, pulse: null },
      virtual: false,
    },
    downloadSpeed: s.speed,
    uploadSpeed: 0,
    downloaded: p.have * engine.meta.pieceLength,
    uploaded: 0,
    unchoked: 0,
    peers: s.peers,
    queued: 0,
    unique: s.peers,
    connectionTries: 0,
    peerSearchRunning: false,
    streamLen: file ? file.length : engine.meta.totalLength,
    streamName: file ? file.path.split("/").pop() : engine.meta.name,
    streamProgress,
    swarmConnections: s.peers,
    swarmPaused: false,
    swarmSize: s.peers,
  });
}

// --- range streaming: the heart of playback ---
async function handleStream(request, infoHash, fileIdxRaw, url) {
  const announce = trackersFromQuery(url);
  // "ws"/"xs" query params carry BEP 19 web seeds and the .torrent URL through
  // the stream URL — the zero-function HTTPS path when the torrent has them.
  const webSeeds = url.searchParams.getAll("ws").filter((u) => /^https?:\/\//i.test(u));
  const xsRaw = url.searchParams.get("xs");
  const xs = xsRaw && /^https?:\/\//i.test(xsRaw) ? xsRaw : null;
  const engine = await ensureEngine(infoHash, announce, webSeeds, xs);
  const file = pickFile(engine, fileIdxRaw);
  if (!file) return json({ error: "no playable file in torrent" }, 404);

  const total = file.length;
  const contentType = guessContentType(file.path);
  const rangeHeader = request.headers.get("range");

  // HEAD (or a plain GET with no Range) — advertise capabilities so the player
  // knows it can seek. No body work needed.
  if (request.method === "HEAD") {
    return new Response(null, {
      status: 200,
      headers: baseStreamHeaders(contentType, total, { acceptRanges: true }),
    });
  }

  if (!rangeHeader) {
    // Some players do an initial full GET; serve from 0 as a 200 stream.
    const stream = engine.readRange(file, 0, total - 1);
    return new Response(stream, {
      status: 200,
      headers: baseStreamHeaders(contentType, total, { acceptRanges: true }),
    });
  }

  const parsed = parseRange(rangeHeader, total);
  if (!parsed) {
    return new Response(null, {
      status: 416,
      headers: { "content-range": `bytes */${total}` },
    });
  }
  const { start, end } = parsed;
  const length = end - start + 1;
  const stream = engine.readRange(file, start, end);
  const headers = baseStreamHeaders(contentType, length, { acceptRanges: true });
  headers.set("content-range", `bytes ${start}-${end}/${total}`);
  return new Response(stream, { status: 206, headers });
}

// --- engine acquisition ---

// Ensure a running engine for infoHash with metadata loaded. Concurrent callers
// share one boot promise. Metadata comes from the IndexedDB cache when present,
// otherwise from /api/metadata (needs peers, which needs trackers/DHT).
function ensureEngine(infoHash, announce, webSeeds, xs) {
  const existing = engines.get(infoHash);
  if (existing && existing.meta) return Promise.resolve(existing);
  const booting = engineBoot.get(infoHash);
  if (booting) return booting;

  const boot = (async () => {
    const engine = await TorrentEngine.open({ onLog: (m) => console.log("[v0][sw]", m) });
    const cached = await engine.store.getSession(infoHash);
    if (cached?.meta?.pieces?.length) {
      // Merge any newly provided web seeds into the cached meta so a repeat
      // stream can still go the zero-function HTTPS route.
      if (webSeeds?.length) {
        cached.meta.webSeeds = [...new Set([...(cached.meta.webSeeds || []), ...webSeeds])];
      }
      await engine.loadMeta(cached.meta, []);
    } else {
      let meta = null;
      // Frugal path: an xs hint is a direct HTTPS URL to the .torrent file —
      // metadata with zero peers and zero function invocations.
      if (xs) {
        try {
          const buf = new Uint8Array(await (await fetch(xs)).arrayBuffer());
          const parsed = await parseTorrent(buf);
          if (parsed.infoHash === infoHash) {
            parsed.webSeeds = [...new Set([...(parsed.webSeeds || []), ...(webSeeds || [])])];
            meta = parsed;
          }
        } catch {
          /* fall through to peer metadata */
        }
      }
      if (!meta) {
        const res = await postJsonSW("/api/metadata", { infoHash, announce });
        if (res.error) throw new Error("metadata: " + res.error);
        meta = await metaFromInfoDict(bytesFromBase64(res.infoBase64), announce, webSeeds);
        if (meta.infoHash !== infoHash) throw new Error("info dict hash mismatch");
        await engine.loadMeta(meta, res.peers || []);
        engines.set(infoHash, engine);
        engineBoot.delete(infoHash);
        return engine;
      }
      await engine.loadMeta(meta, []);
    }
    engines.set(infoHash, engine);
    engineBoot.delete(infoHash);
    return engine;
  })().catch((e) => {
    engineBoot.delete(infoHash);
    throw e;
  });

  engineBoot.set(infoHash, boot);
  return boot;
}

// --- helpers ---

function pickFile(engine, fileIdxRaw) {
  const files = engine.meta.files;
  const idx = parseInt(fileIdxRaw, 10);
  if (Number.isNaN(idx) || idx < 0) {
    // -1 → largest file (stremio-core convention).
    return files.reduce((a, b) => (b.length > a.length ? b : a), files[0]);
  }
  return files[idx] || files[0];
}

function torrentInfo(engine) {
  return {
    infoHash: engine.meta.infoHash,
    name: engine.meta.name,
    files: engine.meta.files.map(fileEntry),
  };
}

function fileEntry(f, i) {
  return { name: f.path.split("/").pop(), path: f.path, length: f.length, offset: f.offset, idx: i };
}

function trackersFromPeerSearch(body) {
  const sources = body?.peerSearch?.sources || body?.sources || [];
  return sources
    .filter((s) => typeof s === "string" && !s.startsWith("dht:"))
    .map((s) => s.replace(/^tracker:/, ""));
}

function trackersFromQuery(url) {
  // Media URLs built by stremio-video carry the peerSearch sources verbatim:
  // "dht:{hash}" entries and "tracker:"-prefixed announce URLs. Keep only
  // real tracker endpoints.
  return url.searchParams
    .getAll("tr")
    .filter((s) => !s.startsWith("dht:"))
    .map((s) => s.replace(/^tracker:/, ""));
}

function normalizeHash(seg) {
  const h = (seg || "").toLowerCase();
  return /^[0-9a-f]{40}$/.test(h) ? h : null;
}

function parseRange(header, total) {
  const m = /^bytes=(\d*)-(\d*)$/.exec(header.trim());
  if (!m) return null;
  let start = m[1] === "" ? null : parseInt(m[1], 10);
  let end = m[2] === "" ? null : parseInt(m[2], 10);
  if (start === null && end === null) return null;
  if (start === null) {
    // suffix range: last N bytes
    start = Math.max(0, total - end);
    end = total - 1;
  } else if (end === null) {
    end = total - 1;
  }
  if (start > end || start < 0 || end >= total) {
    if (start >= total) return null;
    end = Math.min(end, total - 1);
  }
  return { start, end };
}

function baseStreamHeaders(contentType, length, { acceptRanges } = {}) {
  const h = new Headers();
  h.set("content-type", contentType);
  h.set("content-length", String(length));
  if (acceptRanges) h.set("accept-ranges", "bytes");
  h.set("cache-control", "no-store");
  return h;
}

function guessContentType(path) {
  const ext = path.split(".").pop().toLowerCase();
  const map = {
    mp4: "video/mp4",
    m4v: "video/mp4",
    mkv: "video/x-matroska",
    webm: "video/webm",
    avi: "video/x-msvideo",
    mov: "video/quicktime",
    mp3: "audio/mpeg",
    flac: "audio/flac",
    aac: "audio/aac",
    srt: "application/x-subrip",
    vtt: "text/vtt",
  };
  return map[ext] || "application/octet-stream";
}

function json(obj, status = 200) {
  return new Response(JSON.stringify(obj), {
    status,
    headers: { "content-type": "application/json", "cache-control": "no-store" },
  });
}

// SW-context POST helper (engine.js has its own; kept local to avoid coupling).
async function postJsonSW(path, body) {
  const res = await fetch(new URL(path, self.location.origin), {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify(body),
  });
  const text = await res.text();
  try {
    return JSON.parse(text);
  } catch {
    return { error: text.trim().split("\n")[0].slice(0, 200) || `HTTP ${res.status}` };
  }
}
