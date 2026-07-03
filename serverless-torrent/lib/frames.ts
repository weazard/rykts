// Binary streaming frame protocol for /api/download.
//
// The old contract buffered every fetched piece, base64-encoded it, and shipped
// one big JSON blob at the very end of the ~50s invocation. That inflates the
// payload ~33%, forces a giant string allocation + per-piece atob() in the
// browser, and gives the user zero feedback until the function returns.
//
// Instead we stream length-prefixed binary frames over the HTTP response body as
// each piece is verified server-side. The browser parses frames incrementally,
// re-verifies, and persists pieces the moment they arrive. Memory stays flat and
// progress is live.
//
// Wire format (all integers big-endian, unsigned):
//   [magic u32 = 0x53544631 "STF1"]                      -- once, at stream start
//   then a sequence of frames, each:
//     PIECE frame:   [type u8 = 1][index u32][len u32][raw piece bytes]
//     SUMMARY frame: [type u8 = 2][len u32][UTF-8 JSON]  -- once, last
//
// A missing SUMMARY frame means the stream was truncated (function killed / net
// drop); the client keeps whatever verified pieces it already parsed.

export const STREAM_MAGIC = 0x53544631; // "STF1"

export const FrameType = {
  PIECE: 1,
  SUMMARY: 2,
} as const;

// The JSON carried by the SUMMARY frame. Mirrors the old DownloadResponse minus
// the pieces (which now stream as PIECE frames).
export interface DownloadSummary {
  peerHealth: {
    peer: { ip: string; port: number };
    connected: boolean;
    unchoked: boolean;
    piecesServed: number;
    error?: string;
  }[];
  // Peers learned during this round via PEX (ut_pex). The client merges these
  // into its cache for free — no extra announce needed.
  discoveredPeers: { ip: string; port: number }[];
  elapsedMs: number;
  hitDeadline: boolean;
}

// --- big-endian helpers (kept local so this module is import-cycle free) ---

export function writeU32(buf: Uint8Array, off: number, val: number): void {
  buf[off] = (val >>> 24) & 0xff;
  buf[off + 1] = (val >>> 16) & 0xff;
  buf[off + 2] = (val >>> 8) & 0xff;
  buf[off + 3] = val & 0xff;
}

export function readU32(buf: Uint8Array, off: number): number {
  return (
    buf[off] * 0x1000000 + // avoid <<24 sign issues
    (buf[off + 1] << 16) +
    (buf[off + 2] << 8) +
    buf[off + 3]
  );
}

// --- frame encoders (server side) ---

export function encodeMagic(): Uint8Array {
  const out = new Uint8Array(4);
  writeU32(out, 0, STREAM_MAGIC);
  return out;
}

export function encodePieceFrame(index: number, data: Uint8Array): Uint8Array {
  const out = new Uint8Array(1 + 4 + 4 + data.length);
  out[0] = FrameType.PIECE;
  writeU32(out, 1, index);
  writeU32(out, 5, data.length);
  out.set(data, 9);
  return out;
}

export function encodeSummaryFrame(summary: DownloadSummary): Uint8Array {
  const json = new TextEncoder().encode(JSON.stringify(summary));
  const out = new Uint8Array(1 + 4 + json.length);
  out[0] = FrameType.SUMMARY;
  writeU32(out, 1, json.length);
  out.set(json, 5);
  return out;
}

// --- incremental frame parser (client side, but pure so it's unit-testable) ---

export type ParsedFrame =
  | { type: "piece"; index: number; data: Uint8Array }
  | { type: "summary"; summary: DownloadSummary };

// Accumulates streamed bytes and yields whole frames as they complete. Handles
// the leading magic once. TCP/HTTP chunk boundaries do not respect frame
// boundaries, so everything is length-driven.
export class FrameReader {
  private buf: Uint8Array = new Uint8Array(0);
  private magicSeen = false;

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

  *frames(): Generator<ParsedFrame> {
    if (!this.magicSeen) {
      if (this.buf.length < 4) return;
      const magic = readU32(this.buf, 0);
      if (magic !== STREAM_MAGIC) throw new Error("stream: bad magic header");
      this.buf = this.buf.subarray(4);
      this.magicSeen = true;
    }

    while (this.buf.length >= 1) {
      const type = this.buf[0];
      if (type === FrameType.PIECE) {
        if (this.buf.length < 9) return; // need header
        const index = readU32(this.buf, 1);
        const len = readU32(this.buf, 5);
        if (this.buf.length < 9 + len) return; // need body
        const data = this.buf.subarray(9, 9 + len);
        this.buf = this.buf.subarray(9 + len);
        yield { type: "piece", index, data };
      } else if (type === FrameType.SUMMARY) {
        if (this.buf.length < 5) return;
        const len = readU32(this.buf, 1);
        if (this.buf.length < 5 + len) return;
        const json = this.buf.subarray(5, 5 + len);
        this.buf = this.buf.subarray(5 + len);
        yield { type: "summary", summary: JSON.parse(new TextDecoder().decode(json)) };
      } else {
        throw new Error(`stream: unknown frame type ${type}`);
      }
    }
  }
}
