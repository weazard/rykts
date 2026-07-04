// One-shot BEP 5 DHT peer lookup (get_peers) for a single infohash.
//
// Real swarms live mostly on the DHT, not on trackers — trackerless magnets have
// nowhere else to look. A serverless invocation can't run a persistent Kademlia
// node, but it CAN do a bounded iterative lookup: bootstrap from well-known
// routers, repeatedly query the closest-known nodes for `get_peers`, collect any
// `values` (compact peers) they return, and stop at a short deadline. Node state
// is thrown away when the function returns — that's fine, we only want peers.

import dgram from "node:dgram";
import { decode, encode, type Bencodable } from "./bencode.ts";
import { parseCompactPeers } from "./wire.ts";
import { udpAvailable } from "./net-probe.ts";
import type { PeerAddr } from "./types.ts";

const BOOTSTRAP: { host: string; port: number }[] = [
  { host: "router.bittorrent.com", port: 6881 },
  { host: "dht.transmissionbt.com", port: 6881 },
  { host: "router.utorrent.com", port: 6881 },
  { host: "dht.libtorrent.org", port: 25401 },
];

const ALPHA = 4; // parallel in-flight queries (Kademlia concurrency)
const MAX_NODES_QUERIED = 40; // bound the crawl
const DEFAULT_BUDGET_MS = 4000; // DHT runs in parallel with trackers, keep it snappy
const NODE_ID_LEN = 20;

export interface DhtResult {
  peers: PeerAddr[];
  nodesQueried: number;
  error?: string;
}

interface Node {
  id: Uint8Array; // 20-byte node id
  host: string;
  port: number;
  distance: Uint8Array; // XOR(id, infohash)
}

export async function dhtGetPeers(infoHashHex: string, budgetMs = DEFAULT_BUDGET_MS): Promise<DhtResult> {
  // DHT is UDP-only (BEP 5). If the environment drops outbound UDP, fail fast
  // with an actionable reason instead of timing out into a vague "no peers".
  if (!(await udpAvailable())) {
    return {
      peers: [],
      nodesQueried: 0,
      error: "UDP egress blocked in this environment — DHT unavailable (works on Vercel deploys)",
    };
  }
  const infoHash = hexToBytes(infoHashHex);
  const selfId = randomBytes(NODE_ID_LEN);
  const peers = new Map<string, PeerAddr>();

  return new Promise<DhtResult>((resolve) => {
    const sock = dgram.createSocket("udp4");
    let done = false;
    let nodesQueried = 0;
    let inFlight = 0;

    // Candidate nodes sorted by XOR distance to the infohash. We keep querying
    // the closest un-queried ones. `seen` dedups by host:port; `queried` marks
    // nodes we've already sent to.
    const candidates: Node[] = [];
    const seen = new Set<string>();
    const queried = new Set<string>();
    // Outstanding transaction ids → the node we asked (for response routing).
    const pending = new Map<string, Node | null>();

    const finish = (error?: string) => {
      if (done) return;
      done = true;
      clearTimeout(timer);
      try {
        sock.close();
      } catch {
        // already closed
      }
      resolve({ peers: [...peers.values()], nodesQueried, error });
    };
    const timer = setTimeout(() => finish(), budgetMs);

    const addCandidate = (host: string, port: number, id: Uint8Array | null) => {
      const key = `${host}:${port}`;
      if (seen.has(key)) return;
      seen.add(key);
      const nid = id ?? randomBytes(NODE_ID_LEN);
      candidates.push({ id: nid, host, port, distance: xor(nid, infoHash) });
    };

    const send = (node: Node | null, host: string, port: number) => {
      const txid = randomBytes(2);
      const txKey = bytesToLatin1(txid);
      pending.set(txKey, node);
      const query: Record<string, Bencodable> = {
        t: txid,
        y: strBytes("q"),
        q: strBytes("get_peers"),
        a: { id: selfId, info_hash: infoHash },
      };
      inFlight++;
      try {
        sock.send(Buffer.from(encode(query)), port, host, (err) => {
          if (err) {
            inFlight = Math.max(0, inFlight - 1);
            pending.delete(txKey);
            pump();
          }
        });
      } catch {
        inFlight = Math.max(0, inFlight - 1);
        pending.delete(txKey);
      }
    };

    // Issue queries to the closest not-yet-queried candidates, up to ALPHA in
    // flight, until we hit the crawl bound.
    const pump = () => {
      if (done) return;
      if (nodesQueried >= MAX_NODES_QUERIED) {
        if (inFlight === 0) finish();
        return;
      }
      candidates.sort((a, b) => cmp(a.distance, b.distance));
      for (const node of candidates) {
        if (inFlight >= ALPHA) break;
        if (nodesQueried >= MAX_NODES_QUERIED) break;
        const key = `${node.host}:${node.port}`;
        if (queried.has(key)) continue;
        queried.add(key);
        nodesQueried++;
        send(node, node.host, node.port);
      }
      if (inFlight === 0 && nodesQueried > 0) finish();
    };

    sock.on("error", (e) => finish(String(e?.message ?? e)));

    sock.on("message", (msg) => {
      let decoded: Bencodable;
      try {
        decoded = decode(new Uint8Array(msg)).value;
      } catch {
        return;
      }
      if (!isDict(decoded)) return;
      const t = decoded["t"];
      if (t instanceof Uint8Array) {
        const txKey = bytesToLatin1(t);
        if (pending.has(txKey)) pending.delete(txKey);
      }
      inFlight = Math.max(0, inFlight - 1);

      const r = decoded["r"];
      if (isDict(r)) {
        // `values`: list of compact peer strings — the actual answer.
        const values = r["values"];
        if (Array.isArray(values)) {
          for (const v of values) {
            if (v instanceof Uint8Array) {
              for (const p of parseCompactPeers(v)) peers.set(`${p.ip}:${p.port}`, p);
            }
          }
        }
        // `nodes`: 26-byte entries (20 id + 4 ip + 2 port) to crawl next.
        const nodes = r["nodes"];
        if (nodes instanceof Uint8Array) {
          for (let i = 0; i + 26 <= nodes.length; i += 26) {
            const id = nodes.subarray(i, i + 20);
            const ip = `${nodes[i + 20]}.${nodes[i + 21]}.${nodes[i + 22]}.${nodes[i + 23]}`;
            const port = (nodes[i + 24] << 8) | nodes[i + 25];
            if (port > 0) addCandidate(ip, port, id);
          }
        }
      }
      // Enough peers is enough; a few dozen saturates the download loop.
      if (peers.size >= 200) return finish();
      pump();
    });

    // Kick off from the bootstrap routers (their real node ids are unknown, so
    // seed random ids — they still return closer nodes).
    let resolvedBootstraps = 0;
    for (const b of BOOTSTRAP) {
      addCandidate(b.host, b.port, null);
      resolvedBootstraps++;
    }
    if (resolvedBootstraps === 0) return finish("no bootstrap nodes");
    pump();
  });
}

// --- helpers ---

function xor(a: Uint8Array, b: Uint8Array): Uint8Array {
  const out = new Uint8Array(NODE_ID_LEN);
  for (let i = 0; i < NODE_ID_LEN; i++) out[i] = a[i] ^ b[i];
  return out;
}

function cmp(a: Uint8Array, b: Uint8Array): number {
  for (let i = 0; i < a.length; i++) {
    if (a[i] !== b[i]) return a[i] - b[i];
  }
  return 0;
}

function randomBytes(n: number): Uint8Array {
  const out = new Uint8Array(n);
  for (let i = 0; i < n; i++) out[i] = (Math.random() * 256) | 0;
  return out;
}

function hexToBytes(hex: string): Uint8Array {
  const out = new Uint8Array(hex.length / 2);
  for (let i = 0; i < out.length; i++) out[i] = parseInt(hex.substr(i * 2, 2), 16);
  return out;
}

function strBytes(s: string): Uint8Array {
  const out = new Uint8Array(s.length);
  for (let i = 0; i < s.length; i++) out[i] = s.charCodeAt(i);
  return out;
}

function bytesToLatin1(buf: Uint8Array): string {
  let s = "";
  for (let i = 0; i < buf.length; i++) s += String.fromCharCode(buf[i]);
  return s;
}

function isDict(v: Bencodable | undefined): v is Record<string, Bencodable> {
  return !!v && typeof v === "object" && !Array.isArray(v) && !(v instanceof Uint8Array);
}
