// POST /api/download — the stateless bounded downloader.
//
// This function holds no state between invocations. The browser passes in the
// pieces it still needs and its cached peer list; the function connects out over
// TCP and STREAMS each piece back as a binary frame the moment it is fetched
// and SHA-1 verified (see ../lib/frames.ts). The client verifies again and
// persists incrementally — no base64, no end-of-invocation JSON wall.

import type { VercelRequest, VercelResponse } from "@vercel/node";
import { runDownload } from "../lib/download-session.ts";
import { encodeMagic, encodePieceFrame, encodeSummaryFrame } from "../lib/frames.ts";
import type { DownloadRequest } from "../lib/types.ts";

export const config = {
  // Node runtime (raw TCP); give it room but the session self-limits to ~50s.
  maxDuration: 60,
};

export default async function handler(req: VercelRequest, res: VercelResponse): Promise<void> {
  if (req.method !== "POST") {
    res.status(405).json({ error: "POST only" });
    return;
  }

  let body: DownloadRequest;
  try {
    body = (typeof req.body === "string" ? JSON.parse(req.body) : req.body) as DownloadRequest;
  } catch {
    res.status(400).json({ error: "invalid JSON body" });
    return;
  }
  const err = validate(body);
  if (err) {
    res.status(400).json({ error: err });
    return;
  }

  // Errors before this point are normal JSON 4xx. From here on the response is
  // a committed binary stream; a mid-stream failure just truncates it and the
  // client keeps whatever verified pieces it already parsed.
  res.status(200);
  res.setHeader("content-type", "application/octet-stream");
  res.setHeader("cache-control", "no-store");
  res.write(Buffer.from(encodeMagic()));

  try {
    const summary = await runDownload(body, (index, data) => {
      return writeAll(res, Buffer.from(encodePieceFrame(index, data)));
    });
    await writeAll(res, Buffer.from(encodeSummaryFrame(summary)));
  } catch {
    // Stream is already committed; ending without a summary frame signals a
    // truncated round to the client.
  } finally {
    res.end();
  }
}

// res.write with backpressure: wait for 'drain' when the kernel buffer is full
// so large piece bursts don't balloon process memory.
function writeAll(res: VercelResponse, chunk: Buffer): Promise<void> {
  return new Promise((resolve, reject) => {
    const ok = res.write(chunk, (e) => (e ? reject(e) : undefined));
    if (ok) return resolve();
    res.once("drain", resolve);
  });
}

function validate(b: DownloadRequest): string | null {
  if (!b || typeof b !== "object") return "missing body";
  if (!/^[0-9a-f]{40}$/i.test(b.infoHash || "")) return "infoHash must be 40 hex chars";
  if (!Number.isInteger(b.pieceLength) || b.pieceLength <= 0) return "bad pieceLength";
  if (!Number.isInteger(b.totalLength) || b.totalLength <= 0) return "bad totalLength";
  if (!Array.isArray(b.wanted) || b.wanted.length === 0) return "wanted must be a non-empty array";
  if (!Array.isArray(b.peers)) return "peers must be an array";
  for (const w of b.wanted) {
    if (!Number.isInteger(w.index) || w.index < 0) return "bad piece index";
    if (!/^[0-9a-f]{40}$/i.test(w.hash || "")) return "bad piece hash";
  }
  return null;
}
