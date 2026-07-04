// Orchestrates one stateless /api/download invocation: spin up peer connections,
// pull blocks through the scheduler, SHA-1 verify each completed piece, and
// STREAM it to the caller via `emit` the moment it verifies — nothing is
// buffered until the end of the invocation, so memory stays flat no matter how
// many pieces a round moves. Stops at a deadline well under the platform limit.

import { PieceScheduler } from "./piece-scheduler.ts";
import { PeerConnection, type PeerRunResult } from "./peer.ts";
import { sha1Hex, fromHex } from "./torrent.ts";
import type { DownloadSummary } from "./frames.ts";
import type { DownloadRequest, PeerAddr, PeerHealth } from "./types.ts";

// Vercel's default Node function timeout is 60s (configurable). Stay clear of it
// so we always return cleanly with partial results rather than being killed.
const HARD_BUDGET_MS = 50_000;
const MAX_PEERS = 40;
// Cap PEX-discovered peers reported back to the client per round.
const MAX_DISCOVERED = 100;

// `emit` is called (awaited) for each piece that passed server-side SHA-1; the
// caller writes it to the response stream. Returns the summary to send last.
export async function runDownload(
  req: DownloadRequest,
  emit: (index: number, data: Uint8Array) => Promise<void> | void,
): Promise<DownloadSummary> {
  const started = Date.now();
  const budget = Math.min(req.deadlineMs ?? HARD_BUDGET_MS, HARD_BUDGET_MS);
  const deadline = started + budget;

  const infoHash = fromHex(req.infoHash);
  const peerId = req.peerId ? fromHex(req.peerId) : randomPeerId();

  const wantedHash = new Map<number, string>();
  for (const w of req.wanted) wantedHash.set(w.index, w.hash);

  const scheduler = new PieceScheduler(req.wanted, req.pieceLength, req.totalLength);
  const health: PeerHealth[] = [];

  // Peers gossiped to us via ut_pex during this round; returned in the summary
  // so the client's peer cache grows for free with every download call.
  const knownPeers = new Set(req.peers.map((p) => `${p.ip}:${p.port}`));
  const discovered = new Map<string, PeerAddr>();

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
      return new PeerConnection(
        peer,
        infoHash,
        peerId,
        i,
        scheduler,
        {
          onPieceComplete: (index, data) => {
            const expected = wantedHash.get(index);
            if (!expected) return;
            const p = (async () => {
              const got = await sha1Hex(data);
              if (got === expected) {
                scheduler.complete(index);
                // Stream it out immediately; the client re-verifies anyway, so a
                // write error just truncates the stream harmlessly.
                await emit(index, data);
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
          onPeersDiscovered: (pexPeers) => {
            for (const p of pexPeers) {
              const k = `${p.ip}:${p.port}`;
              if (knownPeers.has(k) || discovered.has(k)) continue;
              if (discovered.size >= MAX_DISCOVERED) return;
              discovered.set(k, p);
            }
          },
        },
        deadline,
      );
    });

    if (conns.length === 0) return finish();
    for (const c of conns) c.start();
  });

  return {
    peerHealth: health,
    discoveredPeers: [...discovered.values()],
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

export function randomPeerId(): Uint8Array {
  // Azureus-style client prefix so trackers/peers see a sane id.
  const id = new Uint8Array(20);
  const prefix = "-SL0001-";
  for (let i = 0; i < prefix.length; i++) id[i] = prefix.charCodeAt(i);
  for (let i = prefix.length; i < 20; i++) id[i] = Math.floor(Math.random() * 256);
  return id;
}
