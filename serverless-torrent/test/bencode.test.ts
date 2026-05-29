import test from "node:test";
import assert from "node:assert/strict";
import { decode, encode, bytesToUtf8, type Bencodable } from "../lib/bencode.ts";

const enc = (s: string) => new TextEncoder().encode(s);

test("decode primitives", () => {
  assert.equal(decode(enc("i42e")).value, 42);
  assert.equal(decode(enc("i-7e")).value, -7);
  assert.equal(bytesToUtf8(decode(enc("4:spam")).value as Uint8Array), "spam");
});

test("decode list and dict", () => {
  const list = decode(enc("l4:spami42ee")).value as Bencodable[];
  assert.equal(bytesToUtf8(list[0] as Uint8Array), "spam");
  assert.equal(list[1], 42);

  const dict = decode(enc("d3:cow3:moo4:spam4:eggse")).value as Record<string, Bencodable>;
  assert.equal(bytesToUtf8(dict["cow"] as Uint8Array), "moo");
  assert.equal(bytesToUtf8(dict["spam"] as Uint8Array), "eggs");
});

test("encode round-trips and sorts keys", () => {
  const out = encode({ b: 2, a: enc("x") });
  assert.equal(bytesToUtf8(out), "d1:a1:x1:bi2ee");
});

test("info dict byte range is captured for infohash", () => {
  const src = enc("d4:infod6:lengthi10eee");
  const { infoRange } = decode(src);
  assert.ok(infoRange);
  const slice = bytesToUtf8(src.subarray(infoRange!.start, infoRange!.end));
  assert.equal(slice, "d6:lengthi10ee");
});

test("binary strings survive decode (no utf8 mangling)", () => {
  const raw = new Uint8Array([0x32, 0x3a, 0x00, 0xff]); // "2:\x00\xff"
  const v = decode(raw).value as Uint8Array;
  assert.deepEqual([...v], [0x00, 0xff]);
});
