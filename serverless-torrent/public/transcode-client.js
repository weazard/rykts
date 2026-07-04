// Page-side ffmpeg.wasm transcode backend (the "wasm" variant).
//
// Service Workers cannot spawn Web Workers, so the SW delegates segment
// transcodes to a controlled page via postMessage + MessageChannel. This
// module listens for those requests, lazily boots ffmpeg.wasm (single-thread
// core — no cross-origin isolation requirements), and returns MPEG-TS bytes.
//
// Source access: we fetch(mediaURL) — which routes back through the SW into
// the torrent engine (cache-first). The whole file is pulled into memory and
// mounted in ffmpeg's MEMFS, then each segment is cut with input seeking.
// That makes the first wasm transcode of a file expensive (full download),
// but zero-cost on Vercel functions. This is the trade-off under evaluation
// against the function backend.

let ffmpegPromise = null; // Promise<FFmpeg>
let inputState = null; // { mediaURL, name } — currently mounted input

const FFMPEG_ESM_BASE = "/ffmpeg"; // @ffmpeg/ffmpeg dist/esm
const FFMPEG_UTIL_BASE = "/ffmpeg-util"; // @ffmpeg/util dist/esm
const FFMPEG_CORE_BASE = "/ffmpeg-core"; // @ffmpeg/core dist/esm

async function bootFfmpeg() {
  if (!ffmpegPromise) {
    ffmpegPromise = (async () => {
      const [{ FFmpeg }, { toBlobURL }] = await Promise.all([
        import(`${FFMPEG_ESM_BASE}/index.js`),
        import(`${FFMPEG_UTIL_BASE}/index.js`),
      ]);
      const ffmpeg = new FFmpeg();
      // toBlobURL keeps worker-from-module-origin rules happy regardless of
      // how the assets are served.
      await ffmpeg.load({
        coreURL: await toBlobURL(`${FFMPEG_CORE_BASE}/ffmpeg-core.js`, "text/javascript"),
        wasmURL: await toBlobURL(`${FFMPEG_CORE_BASE}/ffmpeg-core.wasm`, "application/wasm"),
        classWorkerURL: `${FFMPEG_ESM_BASE}/worker.js`,
      });
      return ffmpeg;
    })();
    ffmpegPromise.catch(() => {
      ffmpegPromise = null;
    });
  }
  return ffmpegPromise;
}

async function ensureInput(ffmpeg, mediaURL) {
  if (inputState?.mediaURL === mediaURL) return inputState.name;
  // Swap inputs: drop the previous file to bound memory.
  if (inputState) {
    try {
      await ffmpeg.deleteFile(inputState.name);
    } catch {
      /* already gone */
    }
    inputState = null;
  }
  const res = await fetch(mediaURL); // routes through the SW → engine
  if (!res.ok) throw new Error(`source fetch failed: HTTP ${res.status}`);
  const bytes = new Uint8Array(await res.arrayBuffer());
  const name = "input.bin";
  await ffmpeg.writeFile(name, bytes);
  inputState = { mediaURL, name };
  return name;
}

async function transcodeSegment({ mediaURL, start, dur, vcodec, acodec }) {
  const ffmpeg = await bootFfmpeg();
  const input = await ensureInput(ffmpeg, mediaURL);
  const out = `seg-${start}-${dur}.ts`;
  const code = await ffmpeg.exec([
    "-hide_banner",
    "-loglevel", "error",
    "-ss", String(start),
    "-i", input,
    "-t", String(dur),
    "-c:v", vcodec,
    ...(vcodec === "libx264" ? ["-preset", "ultrafast", "-crf", "26"] : []),
    "-c:a", acodec,
    ...(acodec === "aac" ? ["-b:a", "160k", "-ac", "2"] : []),
    "-muxdelay", "0",
    "-avoid_negative_ts", "make_zero",
    "-f", "mpegts",
    out,
  ]);
  if (code !== 0) throw new Error(`ffmpeg.wasm exited ${code}`);
  const data = await ffmpeg.readFile(out);
  await ffmpeg.deleteFile(out).catch(() => {});
  return data.buffer ?? data; // Uint8Array → ArrayBuffer for transfer
}

// One transcode at a time: ffmpeg.wasm instances are single-tenant.
let queue = Promise.resolve();

export function installTranscodeClient() {
  if (!("serviceWorker" in navigator)) return;
  navigator.serviceWorker.addEventListener("message", (event) => {
    if (event.data?.type !== "v0-transcode") return;
    const port = event.ports[0];
    if (!port) return;
    queue = queue
      .then(() => transcodeSegment(event.data))
      .then(
        (buffer) => port.postMessage({ ok: true, data: buffer }, [buffer]),
        (e) => port.postMessage({ ok: false, error: String(e?.message ?? e) }),
      );
  });
}

installTranscodeClient();
