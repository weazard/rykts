// Tests for the /api/download binary streaming frame protocol. The encoder
// (lib/frames.ts, server) and the parser must agree bit-for-bit — and the
// parser must survive arbitrary TCP/HTTP chunk boundaries.

import test from "node:test";
import assert from "node:assert/strict";
import {
  encodeMagic,
  encodePieceFrame,
  encodeSummaryFrame,
  FrameReader,
  type DownloadSummary,
} from "../lib/frames.ts";

function makeSummary(): DownloadSummary {
  return {
    peerHealth: [
      { peer: { ip: "1.2.3.4", port: 6881 }, connected: true, unchoked: true, piecesServed: 3 },
    ],
    discoveredPeers: [{ ip: "5.6.7.8", port: 51413 }],
    elapsedMs: 1234,
    hitDeadline: false,
  };
}

function concat(parts: Uint8Array[]): Uint8Array {
  const total = parts.reduce((n, p) => n + p.length, 0);
  const out = new Uint8Array(total);
  let o = 0;
  for (const p of parts) {
    out.set(p, o);
    o += p.length;
  }
  return out;
}

test("frames round-trip: pieces then summary", () => {
  const p0 = new Uint8Array(1000).fill(0xaa);
  const p7 = new Uint8Array(65536).fill(0x55);
  const stream = concat([
    encodeMagic(),
    encodePieceFrame(0, p0),
    encodePieceFrame(7, p7),
    encodeSummaryFrame(makeSummary()),
  ]);

  const r = new FrameReader();
  r.push(stream);
  const frames = [...r.frames()];

  assert.equal(frames.length, 3);
  assert.equal(frames[0].type, "piece");
  assert.equal((frames[0] as { index: number }).index, 0);
  assert.deepEqual([...(frames[0] as { data: Uint8Array }).data], [...p0]);
  assert.equal((frames[1] as { index: number }).index, 7);
  assert.equal((frames[1] as { data: Uint8Array }).data.length, 65536);
  assert.equal(frames[2].type, "summary");
  const summary = (frames[2] as { summary: DownloadSummary }).summary;
  assert.equal(summary.elapsedMs, 1234);
  assert.equal(summary.discoveredPeers[0].ip, "5.6.7.8");
});

test("parser survives pathological chunk boundaries", () => {
  const p3 = new Uint8Array(50000);
  for (let i = 0; i < p3.length; i++) p3[i] = (i * 13 + 1) & 0xff;
  const stream = concat([
    encodeMagic(),
    encodePieceFrame(3, p3),
    encodeSummaryFrame(makeSummary()),
  ]);

  // Feed one byte at a time — worst case for any length-driven parser.
  const r = new FrameReader();
  const got: string[] = [];
  for (let i = 0; i < stream.length; i++) {
    r.push(stream.subarray(i, i + 1));
    for (const f of r.frames()) {
      if (f.type === "piece") {
        assert.equal(f.index, 3);
        assert.deepEqual([...f.data], [...p3]);
      }
      got.push(f.type);
    }
  }
  assert.deepEqual(got, ["piece", "summary"]);
});

test("truncated stream yields parsed pieces but no summary", () => {
  const p1 = new Uint8Array(2000).fill(0x42);
  const full = concat([
    encodeMagic(),
    encodePieceFrame(1, p1),
    encodeSummaryFrame(makeSummary()),
  ]);
  // Cut mid-summary, as if the function was killed at the deadline.
  const truncated = full.subarray(0, full.length - 10);

  const r = new FrameReader();
  r.push(truncated);
  const frames = [...r.frames()];
  assert.equal(frames.length, 1, "only the complete piece frame parses");
  assert.equal(frames[0].type, "piece");
});

test("bad magic throws immediately", () => {
  const r = new FrameReader();
  r.push(new Uint8Array([0, 0, 0, 1, 2, 3, 4, 5]));
  assert.throws(() => [...r.frames()], /bad magic/);
});
