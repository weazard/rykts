// Browser-side .torrent parsing — a compact mirror of ../lib/torrent.ts.
// Kept dependency-free so it runs straight from static hosting (no build step).

export function decodeBencode(buf) {
  let pos = 0;
  let infoRange = null;

  function parse(depth) {
    const c = buf[pos];
    if (c === 0x69) return parseInt_(); // 'i'
    if (c === 0x6c) return parseList(depth); // 'l'
    if (c === 0x64) return parseDict(depth); // 'd'
    if (c >= 0x30 && c <= 0x39) return parseStr();
    throw new Error("bencode: unexpected byte at " + pos);
  }
  function ascii(start, end) {
    let s = "";
    for (let i = start; i < end; i++) s += String.fromCharCode(buf[i]);
    return s;
  }
  function parseInt_() {
    pos++;
    let end = pos;
    while (buf[end] !== 0x65) end++;
    const n = Number(ascii(pos, end));
    pos = end + 1;
    return n;
  }
  function parseStr() {
    let end = pos;
    while (buf[end] !== 0x3a) end++;
    const len = Number(ascii(pos, end));
    const start = end + 1;
    const stop = start + len;
    const out = buf.subarray(start, stop);
    pos = stop;
    return out;
  }
  function parseList(depth) {
    pos++;
    const out = [];
    while (buf[pos] !== 0x65) out.push(parse(depth + 1));
    pos++;
    return out;
  }
  function parseDict(depth) {
    pos++;
    const out = {};
    while (buf[pos] !== 0x65) {
      const key = new TextDecoder().decode(parseStr());
      const valStart = pos;
      const val = parse(depth + 1);
      if (depth === 0 && key === "info") infoRange = { start: valStart, end: pos };
      out[key] = val;
    }
    pos++;
    return out;
  }

  const value = parse(0);
  return { value, infoRange };
}

export async function parseTorrent(buf) {
  const { value, infoRange } = decodeBencode(buf);
  if (!infoRange) throw new Error("torrent: missing info dict");
  const infoHash = await sha1Hex(buf.subarray(infoRange.start, infoRange.end));
  const info = value.info;

  const pieceLength = info["piece length"];
  const piecesBlob = info.pieces;
  const pieces = [];
  for (let i = 0; i < piecesBlob.length; i += 20) {
    pieces.push(toHex(piecesBlob.subarray(i, i + 20)));
  }
  const name = new TextDecoder().decode(info.name);

  const files = [];
  let totalLength = 0;
  if (info.files) {
    for (const f of info.files) {
      const path = [name, ...f.path.map((p) => new TextDecoder().decode(p))].join("/");
      files.push({ path, length: f.length, offset: totalLength });
      totalLength += f.length;
    }
  } else {
    files.push({ path: name, length: info.length, offset: 0 });
    totalLength = info.length;
  }

  const announce = new Set();
  if (value.announce) announce.add(new TextDecoder().decode(value.announce));
  if (Array.isArray(value["announce-list"])) {
    for (const tier of value["announce-list"]) {
      for (const u of tier) announce.add(new TextDecoder().decode(u));
    }
  }

  return { infoHash, name, pieceLength, pieces, totalLength, files, announce: [...announce] };
}

// --- magnet links (BEP 9 flow) ---

// Parse a magnet URI into { infoHash (40 hex), name, announce[] }. Supports both
// hex (btih:40 chars) and base32 (btih:32 chars) infohash encodings.
export function parseMagnet(uri) {
  if (!/^magnet:\?/i.test(uri)) throw new Error("not a magnet URI");
  const params = new URLSearchParams(uri.slice(uri.indexOf("?") + 1));
  const xts = params.getAll("xt");
  let infoHash = null;
  for (const xt of xts) {
    const m = /^urn:btih:([0-9a-z]+)$/i.exec(xt);
    if (!m) continue;
    const raw = m[1];
    if (raw.length === 40) infoHash = raw.toLowerCase();
    else if (raw.length === 32) infoHash = base32ToHex(raw);
  }
  if (!infoHash) throw new Error("magnet missing a v1 btih infohash");
  const announce = params.getAll("tr");
  const dn = params.get("dn");
  return { infoHash, name: dn || infoHash, announce };
}

function base32ToHex(b32) {
  const alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZ234567";
  let bits = "";
  for (const ch of b32.toUpperCase()) {
    const idx = alphabet.indexOf(ch);
    if (idx < 0) throw new Error("bad base32 infohash");
    bits += idx.toString(2).padStart(5, "0");
  }
  let hex = "";
  for (let i = 0; i + 8 <= bits.length; i += 8) {
    hex += parseInt(bits.slice(i, i + 8), 2).toString(16).padStart(2, "0");
  }
  return hex.toLowerCase();
}

// Build a TorrentMeta from a raw bencoded info dict (as returned by
// /api/metadata) — the same shape parseTorrent produces from a .torrent file.
export async function metaFromInfoDict(infoBytes, announce) {
  const { value: info } = decodeBencode(infoBytes);
  const infoHash = await sha1Hex(infoBytes);
  const pieceLength = info["piece length"];
  const piecesBlob = info.pieces;
  const pieces = [];
  for (let i = 0; i < piecesBlob.length; i += 20) {
    pieces.push(toHex(piecesBlob.subarray(i, i + 20)));
  }
  const name = new TextDecoder().decode(info.name);
  const files = [];
  let totalLength = 0;
  if (info.files) {
    for (const f of info.files) {
      const path = [name, ...f.path.map((p) => new TextDecoder().decode(p))].join("/");
      files.push({ path, length: f.length, offset: totalLength });
      totalLength += f.length;
    }
  } else {
    files.push({ path: name, length: info.length, offset: 0 });
    totalLength = info.length;
  }
  return { infoHash, name, pieceLength, pieces, totalLength, files, announce: announce || [] };
}

export function bytesFromBase64(b64) {
  const bin = atob(b64);
  const out = new Uint8Array(bin.length);
  for (let i = 0; i < bin.length; i++) out[i] = bin.charCodeAt(i);
  return out;
}

export async function sha1Hex(data) {
  const buf = data.buffer ? data.slice().buffer : data;
  const digest = await crypto.subtle.digest("SHA-1", buf);
  return toHex(new Uint8Array(digest));
}

export function toHex(buf) {
  let s = "";
  for (let i = 0; i < buf.length; i++) s += buf[i].toString(16).padStart(2, "0");
  return s;
}

export function randomPeerIdHex() {
  const id = new Uint8Array(20);
  const prefix = "-SL0001-";
  for (let i = 0; i < prefix.length; i++) id[i] = prefix.charCodeAt(i);
  crypto.getRandomValues(id.subarray(prefix.length));
  return toHex(id);
}
