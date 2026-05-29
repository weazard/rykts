// POST /api/download — the stateless bounded downloader.
//
// This function holds no state between invocations. The browser passes in the
// pieces it still needs and its cached peer list; the function connects out over
// TCP, fetches as many complete pieces as fit in its time budget, and returns
// them (base64) for the client to SHA-1 verify and persist. See ../README.md.

import type { VercelRequest, VercelResponse } from "@vercel/node";
import { runDownload } from "../lib/download-session.ts";
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
  try {
    const body = (typeof req.body === "string" ? JSON.parse(req.body) : req.body) as DownloadRequest;
    const err = validate(body);
    if (err) {
      res.status(400).json({ error: err });
      return;
    }
    const result = await runDownload(body);
    res.status(200).json(result);
  } catch (e) {
    res.status(500).json({ error: String((e as Error)?.message ?? e) });
  }
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
