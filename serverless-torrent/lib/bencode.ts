// Minimal, dependency-free bencode codec.
//
// decode() keeps byte strings as Uint8Array so that binary fields (the 20-byte
// SHA-1 piece hashes, peer compact blobs, etc.) survive intact. It can also
// report the byte range of the top-level `info` value, which is what we SHA-1
// to derive the infohash without re-encoding (re-encoding can change byte order
// and break the hash).

export type Bencodable =
  | number
  | Uint8Array
  | Bencodable[]
  | { [key: string]: Bencodable };

export interface DecodeResult {
  value: Bencodable;
  // Byte range of the `info` dict value within the source, if present at top level.
  infoRange?: { start: number; end: number };
}

const COLON = 0x3a; // :
const E = 0x65; // e
const I = 0x69; // i
const L = 0x6c; // l
const D = 0x64; // d
const ZERO = 0x30; // 0
const NINE = 0x39; // 9

class Decoder {
  buf: Uint8Array;
  pos = 0;
  infoRange?: { start: number; end: number };

  constructor(buf: Uint8Array) {
    this.buf = buf;
  }

  decode(): Bencodable {
    return this.parse(0);
  }

  // depth lets us detect the top-level info dict (depth 1 keys inside depth 0 dict).
  private parse(depth: number): Bencodable {
    const c = this.buf[this.pos];
    if (c === I) return this.parseInt();
    if (c === L) return this.parseList(depth);
    if (c === D) return this.parseDict(depth);
    if (c >= ZERO && c <= NINE) return this.parseStr();
    throw new Error(`bencode: unexpected byte 0x${c?.toString(16)} at ${this.pos}`);
  }

  private parseInt(): number {
    this.pos++; // skip 'i'
    let end = this.pos;
    while (this.buf[end] !== E) {
      if (end >= this.buf.length) throw new Error("bencode: unterminated int");
      end++;
    }
    const n = Number(bytesToAscii(this.buf, this.pos, end));
    this.pos = end + 1;
    if (!Number.isFinite(n)) throw new Error("bencode: bad int");
    return n;
  }

  private parseStr(): Uint8Array {
    let end = this.pos;
    while (this.buf[end] !== COLON) {
      if (end >= this.buf.length) throw new Error("bencode: unterminated string length");
      end++;
    }
    const len = Number(bytesToAscii(this.buf, this.pos, end));
    const start = end + 1;
    const stop = start + len;
    if (stop > this.buf.length) throw new Error("bencode: string overruns buffer");
    const out = this.buf.subarray(start, stop);
    this.pos = stop;
    return out;
  }

  private parseList(depth: number): Bencodable[] {
    this.pos++; // skip 'l'
    const out: Bencodable[] = [];
    while (this.buf[this.pos] !== E) {
      out.push(this.parse(depth + 1));
    }
    this.pos++; // skip 'e'
    return out;
  }

  private parseDict(depth: number): { [key: string]: Bencodable } {
    this.pos++; // skip 'd'
    const out: { [key: string]: Bencodable } = {};
    while (this.buf[this.pos] !== E) {
      const key = bytesToUtf8(this.parseStr());
      const valStart = this.pos;
      const val = this.parse(depth + 1);
      if (depth === 0 && key === "info") {
        this.infoRange = { start: valStart, end: this.pos };
      }
      out[key] = val;
    }
    this.pos++; // skip 'e'
    return out;
  }
}

export function decode(buf: Uint8Array): DecodeResult {
  const d = new Decoder(buf);
  const value = d.decode();
  return { value, infoRange: d.infoRange };
}

export function encode(value: Bencodable): Uint8Array {
  const chunks: Uint8Array[] = [];
  encodeInto(value, chunks);
  return concat(chunks);
}

function encodeInto(value: Bencodable, out: Uint8Array[]): void {
  if (typeof value === "number") {
    if (!Number.isInteger(value)) throw new Error("bencode: cannot encode non-integer number");
    out.push(asciiToBytes(`i${value}e`));
    return;
  }
  if (value instanceof Uint8Array) {
    out.push(asciiToBytes(`${value.length}:`));
    out.push(value);
    return;
  }
  if (typeof value === "string") {
    const bytes = utf8ToBytes(value);
    out.push(asciiToBytes(`${bytes.length}:`));
    out.push(bytes);
    return;
  }
  if (Array.isArray(value)) {
    out.push(asciiToBytes("l"));
    for (const v of value) encodeInto(v, out);
    out.push(asciiToBytes("e"));
    return;
  }
  if (value && typeof value === "object") {
    out.push(asciiToBytes("d"));
    const keys = Object.keys(value).sort();
    for (const k of keys) {
      const kb = utf8ToBytes(k);
      out.push(asciiToBytes(`${kb.length}:`));
      out.push(kb);
      encodeInto((value as Record<string, Bencodable>)[k], out);
    }
    out.push(asciiToBytes("e"));
    return;
  }
  throw new Error("bencode: cannot encode value");
}

// --- byte helpers (Node Buffer not assumed in all call sites) ---

function bytesToAscii(buf: Uint8Array, start: number, end: number): string {
  let s = "";
  for (let i = start; i < end; i++) s += String.fromCharCode(buf[i]);
  return s;
}

export function bytesToUtf8(buf: Uint8Array): string {
  return new TextDecoder().decode(buf);
}

function utf8ToBytes(s: string): Uint8Array {
  return new TextEncoder().encode(s);
}

function asciiToBytes(s: string): Uint8Array {
  const out = new Uint8Array(s.length);
  for (let i = 0; i < s.length; i++) out[i] = s.charCodeAt(i) & 0xff;
  return out;
}

function concat(chunks: Uint8Array[]): Uint8Array {
  let total = 0;
  for (const c of chunks) total += c.length;
  const out = new Uint8Array(total);
  let o = 0;
  for (const c of chunks) {
    out.set(c, o);
    o += c.length;
  }
  return out;
}
