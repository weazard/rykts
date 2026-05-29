// POST /api/announce — peer discovery (the expensive part, kept separate).
//
// The client calls this only when its cached peer list runs low, then reuses the
// returned peers across many cheap /api/download calls. This is the answer to
// "would the function rediscover peers every invocation?" — no: discovery is
// decoupled and amortized.

import type { VercelRequest, VercelResponse } from "@vercel/node";
import { announce } from "../lib/tracker.ts";
import type { AnnounceRequest } from "../lib/types.ts";

export const config = { maxDuration: 30 };

export default async function handler(req: VercelRequest, res: VercelResponse): Promise<void> {
  if (req.method !== "POST") {
    res.status(405).json({ error: "POST only" });
    return;
  }
  try {
    const body = (typeof req.body === "string" ? JSON.parse(req.body) : req.body) as AnnounceRequest;
    if (!/^[0-9a-f]{40}$/i.test(body?.infoHash || "")) {
      res.status(400).json({ error: "infoHash must be 40 hex chars" });
      return;
    }
    if (!Array.isArray(body.announce) || body.announce.length === 0) {
      res.status(400).json({ error: "announce must be a non-empty array of tracker URLs" });
      return;
    }
    const result = await announce(body);
    res.status(200).json(result);
  } catch (e) {
    res.status(500).json({ error: String((e as Error)?.message ?? e) });
  }
}
