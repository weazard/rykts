// POST /api/metadata — magnet link support (BEP 9).
//
// Given only an infohash (from a magnet URI), discover peers via DHT + any
// `tr=` trackers, then pull the bencoded info dict from peers speaking
// ut_metadata. The client turns the returned info bytes into the same
// TorrentMeta shape a .torrent file would produce and runs the normal loop.

import type { VercelRequest, VercelResponse } from "@vercel/node";
import { announce } from "../lib/tracker.ts";
import { fetchMetadata } from "../lib/metadata-session.ts";
import { decode } from "../lib/bencode.ts";
import type { MetadataRequest, MetadataResponse, PeerAddr } from "../lib/types.ts";

export const config = { maxDuration: 60 };

export default async function handler(req: VercelRequest, res: VercelResponse): Promise<void> {
  if (req.method !== "POST") {
    res.status(405).json({ error: "POST only" });
    return;
  }
  try {
    const body = (typeof req.body === "string" ? JSON.parse(req.body) : req.body) as MetadataRequest;
    if (!/^[0-9a-f]{40}$/i.test(body?.infoHash || "")) {
      res.status(400).json({ error: "infoHash must be 40 hex chars" });
      return;
    }
    const infoHash = body.infoHash.toLowerCase();

    // Discover peers unless the caller brought its own.
    let peers: PeerAddr[] = Array.isArray(body.peers) ? body.peers : [];
    if (peers.length < 5) {
      const disc = await announce({
        infoHash,
        announce: Array.isArray(body.announce) ? body.announce : [],
        peerId: body.peerId,
        numWant: 80,
      });
      const seen = new Set(peers.map((p) => `${p.ip}:${p.port}`));
      for (const p of disc.peers) {
        const k = `${p.ip}:${p.port}`;
        if (!seen.has(k)) {
          peers.push(p);
          seen.add(k);
        }
      }
    }
    if (peers.length === 0) {
      res.status(502).json({ error: "no peers found for infohash (DHT + trackers empty)" });
      return;
    }

    const { info } = await fetchMetadata(infoHash, peers);

    // Summarize for the UI without re-implementing full parsing server-side.
    const { value } = decode(info);
    const dict = value as Record<string, unknown>;
    const name =
      dict["name"] instanceof Uint8Array ? new TextDecoder().decode(dict["name"] as Uint8Array) : "unknown";
    const pieceLength = typeof dict["piece length"] === "number" ? (dict["piece length"] as number) : 0;
    let totalLength = 0;
    if (Array.isArray(dict["files"])) {
      for (const f of dict["files"] as { length?: unknown }[]) {
        if (typeof f === "object" && f && typeof (f as Record<string, unknown>)["length"] === "number") {
          totalLength += (f as Record<string, unknown>)["length"] as number;
        }
      }
    } else if (typeof dict["length"] === "number") {
      totalLength = dict["length"] as number;
    }

    const out: MetadataResponse = {
      infoBase64: Buffer.from(info).toString("base64"),
      name,
      totalLength,
      pieceCount: pieceLength > 0 ? Math.ceil(totalLength / pieceLength) : 0,
      peers,
    };
    res.status(200).json(out);
  } catch (e) {
    res.status(500).json({ error: String((e as Error)?.message ?? e) });
  }
}
