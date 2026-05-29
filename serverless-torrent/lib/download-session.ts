// Orchestrates one stateless /api/download invocation: spin up peer connections,
// pull blocks through the scheduler, SHA-1 verify each completed piece, and stop
// at a deadline well under the platform's function time limit.

import { PieceScheduler } from "./piece-scheduler.js";
import { PeerConnection, type PeerRunResult } from "./peer.js";
import { sha1Hex, fromHex } from "./torrent.js";
import type { DownloadRequest, DownloadResponse, DownloadedPiece, PeerHealth } from "./types.js";

// Vercel's default Node function timeout is 60s (configurable). Stay clear of it
// so we always return cleanly with partial results rather than being killed.
const HARD_BUDGET_MS = 50_000;
const MAX_PEERS = 40;

export async function runDownload(req: DownloadRequest): Promise<DownloadResponse> {
  const started = Date.now();
  const budget = Math.min(req.deadlineMs ?? HARD_BUDGET_MS, HARD_BUDGET_MS);
  const deadline = started + budget;

  const infoHash = fromHex(req.infoHash);
  const peerId = req.peerId ? fromHex(req.peerId) : randomPeerId();

  const wantedHash = new Map<number, string>();
  for (const w of req.wanted) wantedHash.set(w.index, w.hash);

  const scheduler = new PieceScheduler(req.wanted, req.pieceLength, req.totalLength);
  const completed = new Map<number, Uint8Array>();
  const health: PeerHealth[] = [];

  const peers = req.peers.slice(0, MAX_PEERS);
  let liveConnections = peers.length;
  const verifying = new Set<Promise<void>>();

  await new Promise<void>((resolve) => {
    let settled = false;
    const finish = () => {
      if (settled) return;
      settled = true;
      clearTimeout(timer);
      for (const c of conns) c.stop();
      // Let any in-flight verifications settle before resolving.
      Promise.allSettled([...verifying]).then(() => resolve());
    };

    const timer = setTimeout(finish, budget);

    const maybeDone = () => {
      if (scheduler.pendingPieceCount === 0) return finish();
      if (liveConnections === 0 && verifying.size === 0) return finish();
    };

    const conns: PeerConnection[] = peers.map((peer, i) => {
      return new PeerConnection(peer, infoHash, peerId, i, scheduler, {
        onPieceComplete: (index, data) => {
          const expected = wantedHash.get(index);
          if (!expected) return;
          const p = (async () => {
            const got = await sha1Hex(data);
            if (got === expected) {
              completed.set(index, data);
              scheduler.complete(index);
            } else {
              // Corrupt / malicious peer — discard and let it be re-fetched.
              scheduler.reset(index);
            }
          })().finally(() => {
            verifying.delete(p);
            maybeDone();
          });
          verifying.add(p);
        },
        onDone: (result: PeerRunResult) => {
          health.push(toHealth(result));
          liveConnections--;
          maybeDone();
        },
      });
    });

    if (conns.length === 0) return finish();
    for (const c of conns) c.start();
  });

  const pieces: DownloadedPiece[] = [];
  for (const [index, data] of completed) {
    pieces.push({ index, data: base64Encode(data) });
  }
  pieces.sort((a, b) => a.index - b.index);

  return {
    pieces,
    peerHealth: health,
    elapsedMs: Date.now() - started,
    hitDeadline: Date.now() >= deadline,
  };
}

function toHealth(r: PeerRunResult): PeerHealth {
  return {
    peer: r.peer,
    connected: r.connected,
    unchoked: r.unchoked,
    piecesServed: r.piecesServed,
    error: r.error,
  };
}

function randomPeerId(): Uint8Array {
  // Azureus-style client prefix so trackers/peers see a sane id.
  const id = new Uint8Array(20);
  const prefix = "-SL0001-";
  for (let i = 0; i < prefix.length; i++) id[i] = prefix.charCodeAt(i);
  for (let i = prefix.length; i < 20; i++) id[i] = Math.floor(Math.random() * 256);
  return id;
}

function base64Encode(data: Uint8Array): string {
  return Buffer.from(data).toString("base64");
}
