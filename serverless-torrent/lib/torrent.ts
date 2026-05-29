// Parse a .torrent metainfo file into a TorrentMeta. Works in Node and the
// browser: SHA-1 is computed via Web Crypto (globalThis.crypto.subtle), which
// exists in both. Binary string fields are kept as Uint8Array by the decoder.

import { decode, bytesToUtf8, type Bencodable } from "./bencode.js";
import type { TorrentMeta, TorrentFile } from "./types.js";

export async function parseTorrent(buf: Uint8Array): Promise<TorrentMeta> {
  const { value, infoRange } = decode(buf);
  if (!isDict(value)) throw new Error("torrent: top level is not a dict");
  if (!infoRange) throw new Error("torrent: missing info dict");

  const infoBytes = buf.subarray(infoRange.start, infoRange.end);
  const infoHash = await sha1Hex(infoBytes);

  const info = value["info"];
  if (!isDict(info)) throw new Error("torrent: info is not a dict");

  const pieceLength = asInt(info["piece length"], "piece length");
  const piecesBlob = asBytes(info["pieces"], "pieces");
  if (piecesBlob.length % 20 !== 0) throw new Error("torrent: pieces length not a multiple of 20");
  const pieces: string[] = [];
  for (let i = 0; i < piecesBlob.length; i += 20) {
    pieces.push(toHex(piecesBlob.subarray(i, i + 20)));
  }

  const name = bytesToUtf8(asBytes(info["name"], "name"));

  const files: TorrentFile[] = [];
  let totalLength = 0;
  if (info["files"]) {
    // multi-file torrent
    const list = info["files"];
    if (!Array.isArray(list)) throw new Error("torrent: files is not a list");
    for (const f of list) {
      if (!isDict(f)) throw new Error("torrent: file entry not a dict");
      const length = asInt(f["length"], "file.length");
      const pathParts = f["path"];
      if (!Array.isArray(pathParts)) throw new Error("torrent: file.path not a list");
      const path = [name, ...pathParts.map((p) => bytesToUtf8(asBytes(p, "path part")))].join("/");
      files.push({ path, length, offset: totalLength });
      totalLength += length;
    }
  } else {
    // single-file torrent
    const length = asInt(info["length"], "length");
    files.push({ path: name, length, offset: 0 });
    totalLength = length;
  }

  const announce = collectAnnounce(value);

  return { infoHash, name, pieceLength, pieces, totalLength, files, announce };
}

function collectAnnounce(top: Record<string, Bencodable>): string[] {
  const urls = new Set<string>();
  if (top["announce"]) urls.add(bytesToUtf8(asBytes(top["announce"], "announce")));
  const tiers = top["announce-list"];
  if (Array.isArray(tiers)) {
    for (const tier of tiers) {
      if (!Array.isArray(tier)) continue;
      for (const u of tier) urls.add(bytesToUtf8(asBytes(u, "announce-list entry")));
    }
  }
  return [...urls];
}

// --- small typed accessors ---

function isDict(v: Bencodable | undefined): v is Record<string, Bencodable> {
  return !!v && typeof v === "object" && !Array.isArray(v) && !(v instanceof Uint8Array);
}
function asInt(v: Bencodable | undefined, field: string): number {
  if (typeof v !== "number") throw new Error(`torrent: ${field} not an int`);
  return v;
}
function asBytes(v: Bencodable | undefined, field: string): Uint8Array {
  if (!(v instanceof Uint8Array)) throw new Error(`torrent: ${field} not a byte string`);
  return v;
}

// --- hashing / hex (portable) ---

export async function sha1Hex(data: Uint8Array): Promise<string> {
  const digest = await globalThis.crypto.subtle.digest("SHA-1", toArrayBuffer(data));
  return toHex(new Uint8Array(digest));
}

function toArrayBuffer(u8: Uint8Array): ArrayBuffer {
  // Copy into a fresh ArrayBuffer so subarray views (with offsets) hash correctly.
  return u8.slice().buffer;
}

export function toHex(buf: Uint8Array): string {
  let s = "";
  for (let i = 0; i < buf.length; i++) s += buf[i].toString(16).padStart(2, "0");
  return s;
}

export function fromHex(hex: string): Uint8Array {
  if (hex.length % 2 !== 0) throw new Error("fromHex: odd length");
  const out = new Uint8Array(hex.length / 2);
  for (let i = 0; i < out.length; i++) out[i] = parseInt(hex.substr(i * 2, 2), 16);
  return out;
}
