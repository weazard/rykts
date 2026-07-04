// BEP 9 magnet metadata fetch: given just an infohash (and some peers), connect
// to peers, negotiate the ut_metadata extension, pull the info dict in 16 KiB
// pieces, verify sha1(info) === infohash, and return the raw info bytes. From
// there the client builds the same TorrentMeta a .torrent file would produce.
//
// Reuses PeerConnection's socket/handshake/extension plumbing — this path just
// ignores pieces and drives the metadata exchange instead.

import { PeerConnection, type ExtensionHandshakeInfo, type MetadataMessage } from "./peer.ts";
import { sha1Hex, fromHex } from "./torrent.ts";
import { randomPeerId } from "./download-session.ts";
import type { PeerAddr } from "./types.ts";

const METADATA_PIECE_SIZE = 16384; // BEP 9 fixes metadata pieces at 16 KiB
const DEFAULT_BUDGET_MS = 25_000;
const MAX_PEERS = 30;
const MAX_METADATA_SIZE = 8 * 1024 * 1024; // sanity cap (8 MiB info dict)

export interface MetadataResult {
  info: Uint8Array; // verified bencoded info dict
}

export async function fetchMetadata(
  infoHashHex: string,
  peers: PeerAddr[],
  budgetMs = DEFAULT_BUDGET_MS,
): Promise<MetadataResult> {
  const infoHash = fromHex(infoHashHex);
  const peerId = randomPeerId();
  const started = Date.now();
  const deadline = started + budgetMs;

  return new Promise<MetadataResult>((resolve, reject) => {
    let settled = false;
    let liveConnections = Math.min(peers.length, MAX_PEERS);

    // Metadata piece buffers, assembled once we know the total size.
    let metadataSize = 0;
    let pieces: (Uint8Array | null)[] = [];
    let received = 0;
    // Track which (conn) we've asked for which piece so we can retry elsewhere.
    const requestedFrom = new Map<number, Set<PeerConnection>>();

    const finish = (err: Error | null, result?: MetadataResult) => {
      if (settled) return;
      settled = true;
      clearTimeout(timer);
      for (const c of conns) c.stop();
      if (err) reject(err);
      else resolve(result!);
    };
    const timer = setTimeout(() => finish(new Error("metadata fetch timeout")), budgetMs);

    const initBuffers = (size: number) => {
      if (metadataSize || size <= 0 || size > MAX_METADATA_SIZE) return;
      metadataSize = size;
      pieces = new Array(Math.ceil(size / METADATA_PIECE_SIZE)).fill(null);
    };

    const requestMissing = (conn: PeerConnection) => {
      if (!metadataSize || !conn.supportsMetadata) return;
      for (let i = 0; i < pieces.length; i++) {
        if (pieces[i]) continue;
        const askedBy = requestedFrom.get(i) ?? new Set<PeerConnection>();
        if (askedBy.has(conn)) continue;
        askedBy.add(conn);
        requestedFrom.set(i, askedBy);
        conn.requestMetadataPiece(i);
        break; // one outstanding piece per peer keeps it simple and fair
      }
    };

    const onExtHandshake = (conn: PeerConnection, m: ExtensionHandshakeInfo) => {
      if (settled) return;
      if (m.metadataSize) initBuffers(m.metadataSize);
      requestMissing(conn);
    };

    const onMetadata = (conn: PeerConnection, msg: MetadataMessage) => {
      if (settled) return;
      if (msg.msgType === 1 && msg.data) {
        if (msg.totalSize) initBuffers(msg.totalSize);
        if (msg.piece >= 0 && msg.piece < pieces.length && !pieces[msg.piece]) {
          pieces[msg.piece] = msg.data;
          received++;
          if (received === pieces.length) return void assemble();
        }
        requestMissing(conn); // pull the next piece from this peer
      } else if (msg.msgType === 2) {
        // reject — try another peer for that piece
        requestMissing(conn);
      }
    };

    const assemble = async () => {
      const total = pieces.reduce((n, p) => n + (p?.length ?? 0), 0);
      const info = new Uint8Array(total);
      let o = 0;
      for (const p of pieces) {
        if (!p) return finish(new Error("metadata assembly: missing piece"));
        info.set(p, o);
        o += p.length;
      }
      const got = await sha1Hex(info);
      if (got !== infoHashHex.toLowerCase()) {
        return finish(new Error("metadata infohash mismatch (corrupt/hostile peer)"));
      }
      finish(null, { info });
    };

    const conns: PeerConnection[] = peers.slice(0, MAX_PEERS).map((peer, i) => {
      return new PeerConnection(
        peer,
        infoHash,
        peerId,
        i,
        null, // no piece scheduler — metadata only
        {
          onPieceComplete: () => {},
          onDone: () => {
            liveConnections--;
            if (liveConnections <= 0 && !metadataSize) {
              finish(new Error("no peer offered metadata"));
            }
          },
          onExtensionHandshake: onExtHandshake,
          onMetadataMessage: onMetadata,
        },
        deadline,
      );
    });

    if (conns.length === 0) return finish(new Error("no peers to fetch metadata from"));
    for (const c of conns) c.start();
  });
}
