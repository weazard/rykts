// GET /api/transcode — one HLS segment via ffmpeg (function backend).
//
// Only usable when the media source is HTTP-reachable from a server (i.e. the
// torrent has a BEP 19 web seed): our /stream/ URLs live inside the user's
// browser and cannot be fetched from here. ffmpeg reads the source over HTTP
// with range-based seeking (-ss before -i), so each invocation downloads only
// the bytes around the requested segment — invoked strictly per segment, on
// demand, to conserve function usage.
//
// Query: src (http/https URL), start (s), dur (s), v (copy|libx264), a (copy|aac)
// Response: video/mp2t stream.

import type { VercelRequest, VercelResponse } from "@vercel/node";
import { spawn } from "node:child_process";
import ffmpegPath from "ffmpeg-static";

export const config = { maxDuration: 60 };

const MAX_SEGMENT_SECONDS = 30;

export default async function handler(req: VercelRequest, res: VercelResponse): Promise<void> {
  if (req.method !== "GET") {
    res.status(405).json({ error: "GET only" });
    return;
  }

  const src = String(req.query.src ?? "");
  const start = Number(req.query.start ?? NaN);
  const dur = Number(req.query.dur ?? NaN);
  const v = req.query.v === "copy" ? "copy" : "libx264";
  const a = req.query.a === "copy" ? "copy" : "aac";

  let parsed: URL;
  try {
    parsed = new URL(src);
  } catch {
    res.status(400).json({ error: "src must be a valid URL" });
    return;
  }
  if (parsed.protocol !== "http:" && parsed.protocol !== "https:") {
    res.status(400).json({ error: "src must be http(s)" });
    return;
  }
  // No SSRF into private ranges: this function only proxies public web seeds.
  if (/^(localhost|127\.|10\.|172\.(1[6-9]|2\d|3[01])\.|192\.168\.|\[::1\])/i.test(parsed.hostname)) {
    res.status(400).json({ error: "src host not allowed" });
    return;
  }
  if (!isFinite(start) || start < 0 || !isFinite(dur) || dur <= 0 || dur > MAX_SEGMENT_SECONDS) {
    res.status(400).json({ error: `start/dur invalid (dur ≤ ${MAX_SEGMENT_SECONDS}s)` });
    return;
  }
  if (!ffmpegPath) {
    res.status(500).json({ error: "ffmpeg binary unavailable" });
    return;
  }

  const args = [
    "-hide_banner",
    "-loglevel", "error",
    // -ss before -i = input seeking (byte-range under the hood for mp4/mkv
    // over HTTP) — ffmpeg fetches only what this segment needs.
    "-ss", String(start),
    "-i", parsed.href,
    "-t", String(dur),
    "-c:v", v,
    ...(v === "libx264" ? ["-preset", "veryfast", "-crf", "23"] : []),
    "-c:a", a,
    ...(a === "aac" ? ["-b:a", "192k", "-ac", "2"] : []),
    "-muxdelay", "0",
    "-avoid_negative_ts", "make_zero",
    "-f", "mpegts",
    "pipe:1",
  ];

  const child = spawn(ffmpegPath, args, { stdio: ["ignore", "pipe", "pipe"] });

  res.status(200);
  res.setHeader("content-type", "video/mp2t");
  res.setHeader("access-control-allow-origin", "*");
  res.setHeader("cache-control", "no-store");

  let stderrTail = "";
  child.stderr.on("data", (d: Buffer) => {
    stderrTail = (stderrTail + d.toString()).slice(-2000);
  });

  child.stdout.pipe(res);

  child.on("close", (code) => {
    if (code !== 0 && !res.writableEnded) {
      console.error("[v0] ffmpeg exited", code, stderrTail);
    }
    res.end();
  });
  child.on("error", (err) => {
    console.error("[v0] ffmpeg spawn error:", err.message);
    if (!res.headersSent) res.status(500).json({ error: "ffmpeg spawn failed" });
    else res.end();
  });
  req.on("close", () => {
    child.kill("SIGKILL");
  });
}
