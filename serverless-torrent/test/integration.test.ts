// End-to-end test of the stateless downloader against a mock TCP seeder that
// speaks the real BitTorrent wire protocol. Proves a /api/download invocation
// connects, requests blocks, reassembles pieces, and SHA-1 verifies them.

import test from "node:test";
import assert from "node:assert/strict";
import net from "node:net";
import { runDownload } from "../lib/download-session.ts";
import { sha1Hex } from "../lib/torrent.ts";
import {
  buildHandshake,
  parseHandshake,
  StreamParser,
  bitfield as bitfieldMsg,
  unchoke as unchokeMsg,
  piece as pieceMsg,
  makeBitfield,
} from "../lib/wire.ts";

const PIECE_LENGTH = 32768; // two 16 KiB blocks
const LAST_LENGTH = 10000; // one partial block
const TOTAL = PIECE_LENGTH + LAST_LENGTH;

function buildData(): Uint8Array {
  const d = new Uint8Array(TOTAL);
  for (let i = 0; i < d.length; i++) d[i] = (i * 31 + 7) & 0xff;
  return d;
}

function pieceSlice(data: Uint8Array, index: number): Uint8Array {
  const start = index * PIECE_LENGTH;
  return data.subarray(start, Math.min(start + PIECE_LENGTH, data.length));
}

// Mock seeder: completes a handshake, advertises both pieces, unchokes on
// interest, and answers REQUESTs with the right bytes.
function startSeeder(data: Uint8Array, infoHash: Uint8Array): Promise<net.Server> {
  return new Promise((resolve) => {
    const server = net.createServer((sock) => {
      const sp = new StreamParser();
      const seederId = new Uint8Array(20).fill(0x11);
      sock.on("data", (chunk) => {
        sp.push(new Uint8Array(chunk.buffer, chunk.byteOffset, chunk.length));
        if (!sp.isHandshakeDone) {
          const hs = sp.takeHandshake();
          if (!hs) return;
          sock.write(buildHandshake(infoHash, seederId));
          sock.write(bitfieldMsg(makeBitfield(2, () => true)));
        }
        for (const m of sp.messages()) {
          if (m.type === "interested") {
            sock.write(unchokeMsg());
          } else if (m.type === "request") {
            const block = pieceSlice(data, m.index).subarray(m.begin, m.begin + m.length);
            sock.write(pieceMsg(m.index, m.begin, block));
          }
        }
      });
      sock.on("error", () => {});
    });
    server.listen(0, "127.0.0.1", () => resolve(server));
  });
}

test("downloads and verifies all pieces from a live mock peer", async () => {
  const data = buildData();
  const infoHashHex = "ab".repeat(20);
  const infoHash = new Uint8Array(20).fill(0xab);
  const server = await startSeeder(data, infoHash);
  const port = (server.address() as net.AddressInfo).port;

  const hash0 = await sha1Hex(pieceSlice(data, 0));
  const hash1 = await sha1Hex(pieceSlice(data, 1));

  const res = await runDownload({
    infoHash: infoHashHex,
    pieceLength: PIECE_LENGTH,
    totalLength: TOTAL,
    wanted: [
      { index: 0, hash: hash0 },
      { index: 1, hash: hash1 },
    ],
    peers: [{ ip: "127.0.0.1", port }],
    deadlineMs: 5000,
  });

  server.close();

  assert.equal(res.pieces.length, 2, "both pieces returned");
  const got0 = Buffer.from(res.pieces[0].data, "base64");
  const got1 = Buffer.from(res.pieces[1].data, "base64");
  assert.deepEqual([...got0], [...pieceSlice(data, 0)], "piece 0 bytes match");
  assert.deepEqual([...got1], [...pieceSlice(data, 1)], "piece 1 bytes match");
  assert.ok(res.peerHealth.some((h) => h.piecesServed === 2));
});

test("rejects a peer that serves corrupt data (no false commit)", async () => {
  const data = buildData();
  const corrupt = data.slice();
  corrupt[100] ^= 0xff; // flip a byte → piece 0 hash will not match
  const infoHashHex = "cd".repeat(20);
  const infoHash = new Uint8Array(20).fill(0xcd);
  const server = await startSeeder(corrupt, infoHash);
  const port = (server.address() as net.AddressInfo).port;

  const realHash0 = await sha1Hex(pieceSlice(data, 0)); // expect the *correct* hash
  const hash1 = await sha1Hex(pieceSlice(data, 1));

  const res = await runDownload({
    infoHash: infoHashHex,
    pieceLength: PIECE_LENGTH,
    totalLength: TOTAL,
    wanted: [
      { index: 0, hash: realHash0 },
      { index: 1, hash: hash1 },
    ],
    peers: [{ ip: "127.0.0.1", port }],
    deadlineMs: 3000,
  });

  server.close();

  // Piece 1 is intact and should come back; piece 0 is corrupt and must be dropped.
  const indices = res.pieces.map((p) => p.index);
  assert.ok(!indices.includes(0), "corrupt piece 0 was not committed");
  assert.ok(indices.includes(1), "intact piece 1 still delivered");
});
