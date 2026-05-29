// Verifies the peer-discovery path the browser hits: announce() must parse
// compact HTTP tracker responses and must degrade gracefully (return JSON, never
// throw) when some trackers are dead or use an unsupported scheme.

import test from "node:test";
import assert from "node:assert/strict";
import http from "node:http";
import { announce } from "../lib/tracker.js";
import { encode } from "../lib/bencode.js";

function compactPeers(peers: { ip: string; port: number }[]): Uint8Array {
  const out = new Uint8Array(peers.length * 6);
  peers.forEach((p, i) => {
    const o = i * 6;
    const parts = p.ip.split(".").map(Number);
    out[o] = parts[0];
    out[o + 1] = parts[1];
    out[o + 2] = parts[2];
    out[o + 3] = parts[3];
    out[o + 4] = (p.port >> 8) & 0xff;
    out[o + 5] = p.port & 0xff;
  });
  return out;
}

function startTracker(peers: { ip: string; port: number }[]): Promise<http.Server> {
  return new Promise((resolve) => {
    const body = encode({ interval: 1800, peers: compactPeers(peers) });
    const server = http.createServer((req, res) => {
      assert.ok(req.url?.includes("info_hash="), "announce must send info_hash");
      assert.ok(req.url?.includes("compact=1"), "announce must request compact peers");
      res.writeHead(200, { "content-type": "text/plain" });
      res.end(Buffer.from(body));
    });
    server.listen(0, "127.0.0.1", () => resolve(server));
  });
}

test("announce parses compact peers from an HTTP tracker", async () => {
  const expected = [
    { ip: "1.2.3.4", port: 6881 },
    { ip: "10.0.0.7", port: 51413 },
  ];
  const server = await startTracker(expected);
  const port = (server.address() as any).port;

  const res = await announce({
    infoHash: "ab".repeat(20),
    announce: [`http://127.0.0.1:${port}/announce`],
    left: 1000,
  });
  server.close();

  assert.deepEqual(res.peers, expected);
  assert.equal(res.trackers[0].ok, true);
  assert.equal(res.trackers[0].peerCount, 2);
});

test("announce degrades gracefully on unsupported/dead trackers", async () => {
  const server = await startTracker([{ ip: "5.6.7.8", port: 6969 }]);
  const port = (server.address() as any).port;

  const res = await announce({
    infoHash: "cd".repeat(20),
    announce: [
      "wss://tracker.openwebtorrent.com", // unsupported scheme
      "http://127.0.0.1:1/announce", // refused connection
      `http://127.0.0.1:${port}/announce`, // works
    ],
  });
  server.close();

  // The working tracker's peer still comes through; failures are reported, not thrown.
  assert.deepEqual(res.peers, [{ ip: "5.6.7.8", port: 6969 }]);
  const okCount = res.trackers.filter((t) => t.ok).length;
  const failCount = res.trackers.filter((t) => !t.ok).length;
  assert.equal(okCount, 1);
  assert.equal(failCount, 2);
  assert.ok(res.trackers.find((t) => t.url.startsWith("wss"))?.error);
});
