// /hlsv2 — HLS transcoding routes for the in-browser streaming server.
//
// stremio-video resolves these against the ORIGIN ROOT (url.resolve with a
// leading "/hlsv2/..."), not under /stream/ — which works because our Service
// Worker controls the whole origin.
//
// Routes (per the real stremio server's contract):
//   GET /hlsv2/probe?mediaURL=...            → ffprobe-shaped JSON (probe.js)
//   GET /hlsv2/{id}/master.m3u8?mediaURL=...&videoCodecs=..&audioCodecs=..
//   GET /hlsv2/{id}/{variant}.m3u8           → segment playlist (VOD)
//   GET /hlsv2/{id}/{n}.ts                   → one transcoded MPEG-TS segment
//
// Two transcode backends, evaluated per media:
//   "function" — GET /api/transcode (ffmpeg-static on Vercel). Only possible
//                when the source is HTTP-reachable from a server (web seed);
//                our /stream/ URLs only exist inside this browser.
//   "wasm"     — ffmpeg.wasm running in the page (SW ↔ page MessageChannel;
//                Service Workers cannot spawn Workers themselves).
// Auto-selection: web seed present → function, else wasm. Override with
// &backend=wasm|function on the master.m3u8 URL (kept for the whole job).
//
// Frugality: probing reads only container headers via the engine (cache-first);
// the function backend is invoked strictly per segment on demand.

import { probeMedia } from "./probe.js";

const SEGMENT_SECONDS = 4;

// Transcode jobs are ephemeral (per SW lifetime). stremio-video generates a
// fresh id per playback attempt, so persistence buys nothing.
const jobs = new Map(); // id → { mediaURL, backend, videoCodecs, audioCodecs }
const probeCache = new Map(); // mediaURL → Promise<probe result>

// resolveMedia: (mediaURL) → { engine, file, webSeedUrl | null } — provided by
// the SW, which owns engine construction and file selection.
export async function handleHls(request, url, resolveMedia) {
  const path = url.pathname.replace(/^\/hlsv2\/?/, "");

  if (path === "probe") {
    return probeHandler(url, resolveMedia);
  }

  const m = /^([^/]+)\/(.+)$/.exec(path);
  if (!m) return json({ error: "bad hlsv2 path" }, 400);
  const [, id, rest] = m;

  if (rest === "master.m3u8") return masterPlaylist(id, url, resolveMedia);
  if (rest.endsWith(".m3u8")) return variantPlaylist(id, resolveMedia);
  const seg = /^(\d+)\.ts$/.exec(rest);
  if (seg) return segment(id, parseInt(seg[1], 10), resolveMedia);
  return json({ error: "not found" }, 404);
}

async function probeHandler(url, resolveMedia) {
  const mediaURL = url.searchParams.get("mediaURL");
  if (!mediaURL) return json({ error: "mediaURL required" }, 400);
  try {
    const result = await cachedProbe(mediaURL, resolveMedia);
    return json(result);
  } catch (e) {
    return json({ error: String(e?.message ?? e) }, 500);
  }
}

function cachedProbe(mediaURL, resolveMedia) {
  let p = probeCache.get(mediaURL);
  if (!p) {
    p = (async () => {
      const { engine, file } = await resolveMedia(mediaURL);
      const read = rangeReader(engine, file);
      return probeMedia(read, file.length);
    })();
    p.catch(() => probeCache.delete(mediaURL));
    probeCache.set(mediaURL, p);
  }
  return p;
}

// Collect engine.readRange (a ReadableStream) into a Uint8Array.
function rangeReader(engine, file) {
  return async (start, endExclusive) => {
    if (endExclusive <= start) return new Uint8Array(0);
    const stream = engine.readRange(file, start, endExclusive - 1);
    const reader = stream.getReader();
    const out = new Uint8Array(endExclusive - start);
    let at = 0;
    for (;;) {
      const { done, value } = await reader.read();
      if (done) break;
      out.set(value.subarray(0, Math.min(value.length, out.length - at)), at);
      at += value.length;
      if (at >= out.length) {
        reader.cancel().catch(() => {});
        break;
      }
    }
    return out;
  };
}

async function masterPlaylist(id, url, resolveMedia) {
  const mediaURL = url.searchParams.get("mediaURL");
  if (!mediaURL) return json({ error: "mediaURL required" }, 400);

  const { webSeedUrl } = await resolveMedia(mediaURL);
  const requested = url.searchParams.get("backend");
  const backend = requested === "wasm" || requested === "function" ? requested : webSeedUrl ? "function" : "wasm";

  jobs.set(id, {
    mediaURL,
    backend,
    videoCodecs: url.searchParams.getAll("videoCodecs"),
    audioCodecs: url.searchParams.getAll("audioCodecs"),
  });

  const body = ["#EXTM3U", "#EXT-X-VERSION:3", "#EXT-X-STREAM-INF:BANDWIDTH=10000000", "variant.m3u8"].join("\n");
  return m3u8(body);
}

async function variantPlaylist(id, resolveMedia) {
  const job = jobs.get(id);
  if (!job) return json({ error: "unknown job" }, 404);

  const probe = await cachedProbe(job.mediaURL, resolveMedia);
  const duration = probe.format.duration;
  if (!duration || !isFinite(duration)) return json({ error: "unknown duration; cannot segment" }, 500);

  const count = Math.max(1, Math.ceil(duration / SEGMENT_SECONDS));
  const lines = [
    "#EXTM3U",
    "#EXT-X-VERSION:3",
    "#EXT-X-PLAYLIST-TYPE:VOD",
    `#EXT-X-TARGETDURATION:${SEGMENT_SECONDS}`,
    "#EXT-X-MEDIA-SEQUENCE:0",
  ];
  for (let i = 0; i < count; i++) {
    const len = i === count - 1 ? duration - SEGMENT_SECONDS * i : SEGMENT_SECONDS;
    lines.push(`#EXTINF:${len.toFixed(3)},`, `${i}.ts`);
  }
  lines.push("#EXT-X-ENDLIST");
  return m3u8(lines.join("\n"));
}

async function segment(id, n, resolveMedia) {
  const job = jobs.get(id);
  if (!job) return json({ error: "unknown job" }, 404);

  const probe = await cachedProbe(job.mediaURL, resolveMedia);
  const start = n * SEGMENT_SECONDS;
  const dur = Math.min(SEGMENT_SECONDS, (probe.format.duration ?? Infinity) - start);
  if (dur <= 0) return json({ error: "segment out of range" }, 404);

  // Copy the source stream when the player already supports its codec —
  // repackaging into TS is enough and orders of magnitude cheaper. Otherwise
  // transcode to the universally supported pair (H.264/AAC).
  const video = probe.streams.find((s) => s.track === "video");
  const audio = probe.streams.find((s) => s.track === "audio");
  const vcodec = video && job.videoCodecs.includes(video.codec) ? "copy" : "libx264";
  const acodec = audio && job.audioCodecs.includes(audio.codec) && (audio.channels ?? 2) <= 2 ? "copy" : "aac";

  if (job.backend === "function") {
    const { webSeedUrl } = await resolveMedia(job.mediaURL);
    if (webSeedUrl) {
      const qs = new URLSearchParams({
        src: webSeedUrl,
        start: String(start),
        dur: String(dur),
        v: vcodec,
        a: acodec,
      });
      // SW-originated fetches bypass the SW itself and hit the network — i.e.
      // this reaches the real /api/transcode function.
      const res = await fetch(`/api/transcode?${qs}`);
      if (res.ok) {
        return new Response(res.body, { status: 200, headers: tsHeaders() });
      }
      // fall through to wasm on function failure
    }
  }

  return wasmSegment(job, start, dur, vcodec, acodec);
}

// Ask a controlled page to run ffmpeg.wasm (SWs can't spawn Workers). The
// page-side counterpart is transcode-client.js, loaded by stremio-web.
async function wasmSegment(job, start, dur, vcodec, acodec) {
  const clientList = await self.clients.matchAll({ type: "window" });
  const client = clientList[0];
  if (!client) return json({ error: "no page available for wasm transcoding" }, 503);

  const data = await new Promise((resolve, reject) => {
    const channel = new MessageChannel();
    const timer = setTimeout(() => reject(new Error("wasm transcode timeout")), 120_000);
    channel.port1.onmessage = (ev) => {
      clearTimeout(timer);
      if (ev.data?.ok) resolve(ev.data.data);
      else reject(new Error(ev.data?.error ?? "wasm transcode failed"));
    };
    client.postMessage(
      { type: "v0-transcode", mediaURL: job.mediaURL, start, dur, vcodec, acodec },
      [channel.port2],
    );
  }).catch((e) => ({ error: String(e?.message ?? e) }));

  if (data?.error) return json({ error: data.error }, 500);
  return new Response(data, { status: 200, headers: tsHeaders() });
}

// --- response helpers ---

function tsHeaders() {
  return {
    "content-type": "video/mp2t",
    "access-control-allow-origin": "*",
    "cache-control": "no-store",
  };
}

function m3u8(body) {
  return new Response(body + "\n", {
    status: 200,
    headers: {
      "content-type": "application/vnd.apple.mpegurl",
      "access-control-allow-origin": "*",
      "cache-control": "no-store",
    },
  });
}

function json(obj, status = 200) {
  return new Response(JSON.stringify(obj), {
    status,
    headers: { "content-type": "application/json", "access-control-allow-origin": "*" },
  });
}
