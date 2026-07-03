// Browser port of lib/frames.ts — incremental parser for the /api/download
// binary stream. Kept dependency-free and in sync with the server encoder:
//   [magic u32 "STF1"]
//   PIECE frame:   [1][index u32][len u32][raw bytes]
//   SUMMARY frame: [2][len u32][UTF-8 JSON]  (always last on a clean stream)
// A stream that ends without a SUMMARY frame was truncated — the caller keeps
// whatever verified pieces it already parsed.

export const STREAM_MAGIC = 0x53544631; // "STF1"

const PIECE = 1;
const SUMMARY = 2;

function readU32(buf, off) {
  return buf[off] * 0x1000000 + (buf[off + 1] << 16) + (buf[off + 2] << 8) + buf[off + 3];
}

export class FrameReader {
  constructor() {
    this.buf = new Uint8Array(0);
    this.magicSeen = false;
  }

  push(chunk) {
    if (this.buf.length === 0) {
      this.buf = chunk;
    } else {
      const merged = new Uint8Array(this.buf.length + chunk.length);
      merged.set(this.buf, 0);
      merged.set(chunk, this.buf.length);
      this.buf = merged;
    }
  }

  // Yields { type: "piece", index, data } and { type: "summary", summary }.
  *frames() {
    if (!this.magicSeen) {
      if (this.buf.length < 4) return;
      if (readU32(this.buf, 0) !== STREAM_MAGIC) throw new Error("stream: bad magic header");
      this.buf = this.buf.subarray(4);
      this.magicSeen = true;
    }

    while (this.buf.length >= 1) {
      const type = this.buf[0];
      if (type === PIECE) {
        if (this.buf.length < 9) return;
        const index = readU32(this.buf, 1);
        const len = readU32(this.buf, 5);
        if (this.buf.length < 9 + len) return;
        const data = this.buf.subarray(9, 9 + len);
        this.buf = this.buf.subarray(9 + len);
        yield { type: "piece", index, data };
      } else if (type === SUMMARY) {
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
