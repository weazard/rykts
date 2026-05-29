// Tracker announce for peer discovery. Supports HTTP(S) (BEP 3, compact peers)
// and UDP (BEP 15). This is the *expensive* part of BitTorrent, so it lives in
// its own function and is called infrequently by the client, which caches the
// returned peers and reuses them across many cheap /api/download calls.

import dgram from "node:dgram";
import { decode, type Bencodable } from "./bencode.js";
import { fromHex } from "./torrent.js";
import type { AnnounceRequest, AnnounceResponse, PeerAddr } from "./types.js";

export async function announce(req: AnnounceRequest): Promise<AnnounceResponse> {
  const peerId = req.peerId ? fromHex(req.peerId) : defaultPeerId();
  const infoHash = fromHex(req.infoHash);
  const trackers: AnnounceResponse["trackers"] = [];
  const dedup = new Map<string, PeerAddr>();

  const results = await Promise.allSettled(
    req.announce.map((url) => announceOne(url, infoHash, peerId, req)),
  );

  results.forEach((r, i) => {
    const url = req.announce[i];
    if (r.status === "fulfilled") {
      for (const p of r.value) dedup.set(`${p.ip}:${p.port}`, p);
      trackers.push({ url, ok: true, peerCount: r.value.length });
    } else {
      trackers.push({ url, ok: false, peerCount: 0, error: String(r.reason?.message ?? r.reason) });
    }
  });

  return { peers: [...dedup.values()], trackers };
}

function announceOne(
  url: string,
  infoHash: Uint8Array,
  peerId: Uint8Array,
  req: AnnounceRequest,
): Promise<PeerAddr[]> {
  if (url.startsWith("udp:")) return announceUdp(url, infoHash, peerId, req);
  if (url.startsWith("http:") || url.startsWith("https:")) {
    return announceHttp(url, infoHash, peerId, req);
  }
  return Promise.reject(new Error(`unsupported tracker scheme: ${url}`));
}

// --- HTTP(S) ---

async function announceHttp(
  url: string,
  infoHash: Uint8Array,
  peerId: Uint8Array,
  req: AnnounceRequest,
): Promise<PeerAddr[]> {
  const u = new URL(url);
  const qs =
    `info_hash=${urlEncodeBytes(infoHash)}` +
    `&peer_id=${urlEncodeBytes(peerId)}` +
    `&port=${req.port ?? 6881}` +
    `&uploaded=${req.uploaded ?? 0}` +
    `&downloaded=${req.downloaded ?? 0}` +
    `&left=${req.left ?? 0}` +
    `&numwant=${req.numWant ?? 80}` +
    `&compact=1&event=started`;
  u.search = (u.search ? u.search.slice(1) + "&" : "") + qs;

  const ctrl = new AbortController();
  const t = setTimeout(() => ctrl.abort(), 8000);
  try {
    const res = await fetch(u.toString(), { signal: ctrl.signal });
    const body = new Uint8Array(await res.arrayBuffer());
    const { value } = decode(body);
    if (!isDict(value)) throw new Error("tracker response not a dict");
    if (value["failure reason"] instanceof Uint8Array) {
      throw new Error(new TextDecoder().decode(value["failure reason"]));
    }
    return parsePeersField(value["peers"]);
  } finally {
    clearTimeout(t);
  }
}

function parsePeersField(peers: Bencodable | undefined): PeerAddr[] {
  if (peers instanceof Uint8Array) return parseCompactPeers(peers);
  if (Array.isArray(peers)) {
    // non-compact: list of dicts {ip, port}
    const out: PeerAddr[] = [];
    for (const p of peers) {
      if (!isDict(p)) continue;
      const ip = p["ip"];
      const port = p["port"];
      if (ip instanceof Uint8Array && typeof port === "number") {
        out.push({ ip: new TextDecoder().decode(ip), port });
      }
    }
    return out;
  }
  return [];
}

function parseCompactPeers(buf: Uint8Array): PeerAddr[] {
  const out: PeerAddr[] = [];
  for (let i = 0; i + 6 <= buf.length; i += 6) {
    const ip = `${buf[i]}.${buf[i + 1]}.${buf[i + 2]}.${buf[i + 3]}`;
    const port = (buf[i + 4] << 8) | buf[i + 5];
    out.push({ ip, port });
  }
  return out;
}

// --- UDP (BEP 15) ---

async function announceUdp(
  url: string,
  infoHash: Uint8Array,
  peerId: Uint8Array,
  req: AnnounceRequest,
): Promise<PeerAddr[]> {
  const u = new URL(url);
  const host = u.hostname;
  const port = Number(u.port || 80);

  return new Promise<PeerAddr[]>((resolve, reject) => {
    const sock = dgram.createSocket("udp4");
    let done = false;
    const finish = (err: Error | null, peers: PeerAddr[] = []) => {
      if (done) return;
      done = true;
      clearTimeout(timer);
      sock.close();
      err ? reject(err) : resolve(peers);
    };
    const timer = setTimeout(() => finish(new Error("udp tracker timeout")), 8000);

    const transactionId = (Math.random() * 0xffffffff) >>> 0;
    const CONNECT_MAGIC = 0x41727101980n;

    // connect request: magic(8) action(4=0) txid(4)
    const connectReq = Buffer.alloc(16);
    connectReq.writeBigUInt64BE(CONNECT_MAGIC, 0);
    connectReq.writeUInt32BE(0, 8);
    connectReq.writeUInt32BE(transactionId, 12);

    sock.on("error", (e) => finish(e));
    sock.on("message", (msg) => {
      if (msg.length < 8) return;
      const action = msg.readUInt32BE(0);
      const txid = msg.readUInt32BE(4);
      if (txid !== transactionId) return;
      if (action === 0 && msg.length >= 16) {
        // connect response → send announce
        const connectionId = msg.subarray(8, 16);
        sock.send(buildUdpAnnounce(connectionId, transactionId, infoHash, peerId, req), port, host);
      } else if (action === 1 && msg.length >= 20) {
        // announce response: action(4) txid(4) interval(4) leechers(4) seeders(4) [ip(4) port(2)]*
        const peers: PeerAddr[] = [];
        for (let i = 20; i + 6 <= msg.length; i += 6) {
          peers.push({
            ip: `${msg[i]}.${msg[i + 1]}.${msg[i + 2]}.${msg[i + 3]}`,
            port: (msg[i + 4] << 8) | msg[i + 5],
          });
        }
        finish(null, peers);
      } else if (action === 3) {
        finish(new Error("udp tracker error: " + msg.subarray(8).toString()));
      }
    });

    sock.send(connectReq, port, host);
  });
}

function buildUdpAnnounce(
  connectionId: Uint8Array,
  transactionId: number,
  infoHash: Uint8Array,
  peerId: Uint8Array,
  req: AnnounceRequest,
): Buffer {
  const b = Buffer.alloc(98);
  Buffer.from(connectionId).copy(b, 0); // connection_id (8)
  b.writeUInt32BE(1, 8); // action = announce
  b.writeUInt32BE(transactionId, 12);
  Buffer.from(infoHash).copy(b, 16); // info_hash (20)
  Buffer.from(peerId).copy(b, 36); // peer_id (20)
  b.writeBigUInt64BE(BigInt(req.downloaded ?? 0), 56);
  b.writeBigUInt64BE(BigInt(req.left ?? 0), 64);
  b.writeBigUInt64BE(BigInt(req.uploaded ?? 0), 72);
  b.writeUInt32BE(2, 80); // event = started
  b.writeUInt32BE(0, 84); // ip = default
  b.writeUInt32BE((Math.random() * 0xffffffff) >>> 0, 88); // key
  b.writeInt32BE(req.numWant ?? 80, 92); // num_want
  b.writeUInt16BE(req.port ?? 6881, 96); // port
  return b;
}

// --- helpers ---

function urlEncodeBytes(bytes: Uint8Array): string {
  let s = "";
  for (const b of bytes) s += "%" + b.toString(16).padStart(2, "0");
  return s;
}

function isDict(v: Bencodable | undefined): v is Record<string, Bencodable> {
  return !!v && typeof v === "object" && !Array.isArray(v) && !(v instanceof Uint8Array);
}

function defaultPeerId(): Uint8Array {
  const id = new Uint8Array(20);
  const prefix = "-SL0001-";
  for (let i = 0; i < prefix.length; i++) id[i] = prefix.charCodeAt(i);
  for (let i = prefix.length; i < 20; i++) id[i] = Math.floor(Math.random() * 256);
  return id;
}
