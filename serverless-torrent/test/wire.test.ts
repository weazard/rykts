import test from "node:test";
import assert from "node:assert/strict";
import {
  buildHandshake,
  parseHandshake,
  StreamParser,
  request,
  piece,
  have,
  bitfield,
  unchoke,
  bitfieldHas,
  makeBitfield,
  readUint32,
  writeUint32,
} from "../lib/wire.ts";

const ih = new Uint8Array(20).fill(0xab);
const pid = new Uint8Array(20).fill(0xcd);

test("handshake round-trip", () => {
  const hs = buildHandshake(ih, pid);
  assert.equal(hs.length, 68);
  const parsed = parseHandshake(hs);
  assert.ok(parsed);
  assert.deepEqual([...parsed!.hs.infoHash], [...ih]);
  assert.deepEqual([...parsed!.hs.peerId], [...pid]);
  assert.equal(parsed!.consumed, 68);
});

test("parseHandshake waits for full buffer", () => {
  const hs = buildHandshake(ih, pid);
  assert.equal(parseHandshake(hs.subarray(0, 40)), null);
});

test("uint32 round-trip handles high bit", () => {
  const b = new Uint8Array(4);
  writeUint32(b, 0, 0xfffffff0);
  assert.equal(readUint32(b, 0), 0xfffffff0);
});

test("StreamParser reassembles split messages", () => {
  const sp = new StreamParser();
  // feed a handshake split across two chunks first
  const hs = buildHandshake(ih, pid);
  sp.push(hs.subarray(0, 30));
  assert.equal(sp.takeHandshake(), null);
  sp.push(hs.subarray(30));
  assert.ok(sp.takeHandshake());

  // now two messages, delivered byte-fragmented
  const stream = concat([unchoke(), have(5)]);
  for (const byte of stream) sp.push(new Uint8Array([byte]));
  const msgs = [...sp.messages()];
  assert.equal(msgs[0].type, "unchoke");
  assert.equal(msgs[1].type, "have");
  assert.equal((msgs[1] as any).index, 5);
});

test("request and piece encode/decode", () => {
  const sp = new StreamParser();
  // skip handshake gating
  sp.push(buildHandshake(ih, pid));
  sp.takeHandshake();

  const block = new Uint8Array([1, 2, 3, 4]);
  sp.push(concat([request(2, 16384, 16384), piece(2, 16384, block)]));
  const msgs = [...sp.messages()];
  assert.equal(msgs[0].type, "request");
  assert.deepEqual(
    { i: (msgs[0] as any).index, b: (msgs[0] as any).begin, l: (msgs[0] as any).length },
    { i: 2, b: 16384, l: 16384 },
  );
  assert.equal(msgs[1].type, "piece");
  assert.deepEqual([...(msgs[1] as any).block], [1, 2, 3, 4]);
});

test("bitfield helpers are MSB-first", () => {
  const bits = makeBitfield(10, (i) => i === 0 || i === 9);
  assert.ok(bitfieldHas(bits, 0));
  assert.ok(bitfieldHas(bits, 9));
  assert.ok(!bitfieldHas(bits, 1));
  assert.ok(!bitfieldHas(bits, 8));
  // round-trip through a bitfield message
  const sp = new StreamParser();
  sp.push(buildHandshake(ih, pid));
  sp.takeHandshake();
  sp.push(bitfield(bits));
  const m = [...sp.messages()][0];
  assert.equal(m.type, "bitfield");
  assert.deepEqual([...(m as any).bits], [...bits]);
});

function concat(arrs: Uint8Array[]): Uint8Array {
  let n = 0;
  for (const a of arrs) n += a.length;
  const out = new Uint8Array(n);
  let o = 0;
  for (const a of arrs) {
    out.set(a, o);
    o += a.length;
  }
  return out;
}
