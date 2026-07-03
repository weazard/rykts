// BitTorrent peer wire protocol (BEP 3) message framing — pure functions, no I/O.
// Encoders build buffers to send; the StreamParser reassembles the length-prefixed
// message stream that arrives over TCP (which does not respect message boundaries).

export const PROTOCOL = "BitTorrent protocol";
export const BLOCK_SIZE = 16384; // 16 KiB, the standard request block size

export const MsgId = {
  CHOKE: 0,
  UNCHOKE: 1,
  INTERESTED: 2,
  NOT_INTERESTED: 3,
  HAVE: 4,
  BITFIELD: 5,
  REQUEST: 6,
  PIECE: 7,
  CANCEL: 8,
  PORT: 9,
  EXTENDED: 20, // BEP 10 extension protocol
} as const;

// BEP 10: reserved byte 5, bit 0x10 signals extension protocol support.
export const EXTENSION_RESERVED_BYTE = 5;
export const EXTENSION_RESERVED_BIT = 0x10;

export interface Handshake {
  infoHash: Uint8Array; // 20 bytes
  peerId: Uint8Array; // 20 bytes
  reserved: Uint8Array; // 8 bytes
}

export function buildHandshake(infoHash: Uint8Array, peerId: Uint8Array): Uint8Array {
  if (infoHash.length !== 20) throw new Error("handshake: infoHash must be 20 bytes");
  if (peerId.length !== 20) throw new Error("handshake: peerId must be 20 bytes");
  const out = new Uint8Array(49 + PROTOCOL.length);
  let o = 0;
  out[o++] = PROTOCOL.length; // pstrlen = 19
  for (let i = 0; i < PROTOCOL.length; i++) out[o++] = PROTOCOL.charCodeAt(i);
  // Advertise BEP 10 extension protocol support (ut_pex, ut_metadata).
  out[o + EXTENSION_RESERVED_BYTE] = EXTENSION_RESERVED_BIT;
  o += 8;
  out.set(infoHash, o);
  o += 20;
  out.set(peerId, o);
  return out;
}

// Returns the parsed handshake and the number of bytes consumed, or null if the
// 68-byte handshake is not fully buffered yet.
export function parseHandshake(buf: Uint8Array): { hs: Handshake; consumed: number } | null {
  if (buf.length < 1) return null;
  const pstrlen = buf[0];
  const total = 49 + pstrlen;
  if (buf.length < total) return null;
  const reserved = buf.subarray(1 + pstrlen, 1 + pstrlen + 8);
  const infoHash = buf.subarray(1 + pstrlen + 8, 1 + pstrlen + 28);
  const peerId = buf.subarray(1 + pstrlen + 28, 1 + pstrlen + 48);
  return { hs: { infoHash, peerId, reserved }, consumed: total };
}

// --- message encoders ---

function msg(id: number, payload?: Uint8Array): Uint8Array {
  const plen = payload ? payload.length : 0;
  const out = new Uint8Array(4 + 1 + plen);
  writeUint32(out, 0, 1 + plen);
  out[4] = id;
  if (payload) out.set(payload, 5);
  return out;
}

export const keepAlive = (): Uint8Array => new Uint8Array([0, 0, 0, 0]);
export const choke = (): Uint8Array => msg(MsgId.CHOKE);
export const unchoke = (): Uint8Array => msg(MsgId.UNCHOKE);
export const interested = (): Uint8Array => msg(MsgId.INTERESTED);
export const notInterested = (): Uint8Array => msg(MsgId.NOT_INTERESTED);

export function have(index: number): Uint8Array {
  const p = new Uint8Array(4);
  writeUint32(p, 0, index);
  return msg(MsgId.HAVE, p);
}

export function bitfield(bits: Uint8Array): Uint8Array {
  return msg(MsgId.BITFIELD, bits);
}

export function request(index: number, begin: number, length: number): Uint8Array {
  const p = new Uint8Array(12);
  writeUint32(p, 0, index);
  writeUint32(p, 4, begin);
  writeUint32(p, 8, length);
  return msg(MsgId.REQUEST, p);
}

export function piece(index: number, begin: number, block: Uint8Array): Uint8Array {
  const p = new Uint8Array(8 + block.length);
  writeUint32(p, 0, index);
  writeUint32(p, 4, begin);
  p.set(block, 8);
  return msg(MsgId.PIECE, p);
}

// BEP 10 extended message: <len><id=20><ext id u8><payload>.
// ext id 0 = extension handshake; otherwise the id the receiver advertised in
// its handshake `m` dict.
export function extended(extId: number, payload: Uint8Array): Uint8Array {
  const p = new Uint8Array(1 + payload.length);
  p[0] = extId;
  p.set(payload, 1);
  return msg(MsgId.EXTENDED, p);
}

export function handshakeSupportsExtensions(hs: Handshake): boolean {
  return (hs.reserved[EXTENSION_RESERVED_BYTE] & EXTENSION_RESERVED_BIT) !== 0;
}

// --- incoming message parsing ---

export type ParsedMessage =
  | { type: "keepAlive" }
  | { type: "choke" }
  | { type: "unchoke" }
  | { type: "interested" }
  | { type: "notInterested" }
  | { type: "have"; index: number }
  | { type: "bitfield"; bits: Uint8Array }
  | { type: "request"; index: number; begin: number; length: number }
  | { type: "piece"; index: number; begin: number; block: Uint8Array }
  | { type: "cancel"; index: number; begin: number; length: number }
  | { type: "port"; port: number }
  | { type: "extended"; extId: number; payload: Uint8Array }
  | { type: "unknown"; id: number; payload: Uint8Array };

// Accumulates raw TCP bytes and yields whole protocol messages. The handshake is
// handled separately (call setHandshakeDone() once the 68-byte handshake is read).
export class StreamParser {
  private buf: Uint8Array = new Uint8Array(0);
  private handshakeDone = false;

  push(chunk: Uint8Array): void {
    if (this.buf.length === 0) {
      this.buf = chunk;
    } else {
      const merged = new Uint8Array(this.buf.length + chunk.length);
      merged.set(this.buf, 0);
      merged.set(chunk, this.buf.length);
      this.buf = merged;
    }
  }

  // Pull a handshake off the front if present.
  takeHandshake(): Handshake | null {
    const r = parseHandshake(this.buf);
    if (!r) return null;
    this.buf = this.buf.subarray(r.consumed);
    this.handshakeDone = true;
    return r.hs;
  }

  get isHandshakeDone(): boolean {
    return this.handshakeDone;
  }

  // Yield all complete messages currently buffered.
  *messages(): Generator<ParsedMessage> {
    while (this.buf.length >= 4) {
      const len = readUint32(this.buf, 0);
      if (this.buf.length < 4 + len) break; // wait for more bytes
      const frame = this.buf.subarray(4, 4 + len);
      this.buf = this.buf.subarray(4 + len);
      if (len === 0) {
        yield { type: "keepAlive" };
        continue;
      }
      yield decodeMessage(frame);
    }
  }
}

function decodeMessage(frame: Uint8Array): ParsedMessage {
  const id = frame[0];
  const payload = frame.subarray(1);
  switch (id) {
    case MsgId.CHOKE:
      return { type: "choke" };
    case MsgId.UNCHOKE:
      return { type: "unchoke" };
    case MsgId.INTERESTED:
      return { type: "interested" };
    case MsgId.NOT_INTERESTED:
      return { type: "notInterested" };
    case MsgId.HAVE:
      return { type: "have", index: readUint32(payload, 0) };
    case MsgId.BITFIELD:
      return { type: "bitfield", bits: payload };
    case MsgId.REQUEST:
      return {
        type: "request",
        index: readUint32(payload, 0),
        begin: readUint32(payload, 4),
        length: readUint32(payload, 8),
      };
    case MsgId.PIECE:
      return {
        type: "piece",
        index: readUint32(payload, 0),
        begin: readUint32(payload, 4),
        block: payload.subarray(8),
      };
    case MsgId.CANCEL:
      return {
        type: "cancel",
        index: readUint32(payload, 0),
        begin: readUint32(payload, 4),
        length: readUint32(payload, 8),
      };
    case MsgId.PORT:
      return { type: "port", port: (payload[0] << 8) | payload[1] };
    case MsgId.EXTENDED:
      return { type: "extended", extId: payload[0], payload: payload.subarray(1) };
    default:
      return { type: "unknown", id, payload };
  }
}

// --- compact peer parsing (shared by tracker, PEX, DHT) ---

// 6 bytes per peer: 4-byte IPv4 + 2-byte big-endian port.
export function parseCompactPeers(buf: Uint8Array): { ip: string; port: number }[] {
  const out: { ip: string; port: number }[] = [];
  for (let i = 0; i + 6 <= buf.length; i += 6) {
    const port = (buf[i + 4] << 8) | buf[i + 5];
    if (port === 0) continue;
    out.push({ ip: `${buf[i]}.${buf[i + 1]}.${buf[i + 2]}.${buf[i + 3]}`, port });
  }
  return out;
}

// --- bitfield helpers (MSB-first, per spec) ---

export function bitfieldHas(bits: Uint8Array, index: number): boolean {
  const byte = index >> 3;
  if (byte >= bits.length) return false;
  return (bits[byte] & (0x80 >> (index & 7))) !== 0;
}

export function makeBitfield(numPieces: number, has: (i: number) => boolean): Uint8Array {
  const bytes = new Uint8Array(Math.ceil(numPieces / 8));
  for (let i = 0; i < numPieces; i++) {
    if (has(i)) bytes[i >> 3] |= 0x80 >> (i & 7);
  }
  return bytes;
}

// --- endian helpers ---

export function writeUint32(buf: Uint8Array, off: number, val: number): void {
  buf[off] = (val >>> 24) & 0xff;
  buf[off + 1] = (val >>> 16) & 0xff;
  buf[off + 2] = (val >>> 8) & 0xff;
  buf[off + 3] = val & 0xff;
}

export function readUint32(buf: Uint8Array, off: number): number {
  return (
    (buf[off] * 0x1000000) + // avoid sign issues with <<24
    (buf[off + 1] << 16) +
    (buf[off + 2] << 8) +
    buf[off + 3]
  );
}
