# serverless-torrent

A **semi-stateless, serverless-native BitTorrent leech** designed for Vercel.

The premise: mainstream BitTorrent clients are stateful long-lived processes that
own a TCP/IP stack and a persistent peer set. Serverless functions are the
opposite — they live <60 s, listen on nothing, and forget everything when they
return. This project splits the client in two so that the half that *needs* to be
stateful runs in the browser, and the half that needs raw outbound TCP runs in a
disposable function.

```
┌─────────────────────────── Browser (durable, long-lived) ────────────────────────────┐
│  • parses .torrent files OR magnet links (metadata fetched via BEP-9)                 │
│  • IndexedDB: verified piece Blobs + bitfield + cached peer list + stable peer id     │
│  • worker pool: 1-4 parallel /api/download invocations on disjoint piece ranges       │
│    (concurrency configurable, default adaptive based on measured throughput)          │
└──────┬─────────────────────────┬──────────────────────────────┬───────────────────────┘
       │ POST /api/announce      │ POST /api/metadata (once     │ POST /api/download (hot
       │ (rare)                  │ per magnet)                  │ path, streaming, xN)
       ▼                         ▼                              ▼
┌──────────────────┐  ┌───────────────────────┐  ┌─────────────────────────────────────┐
│ HTTP/UDP tracker │  │ ut_metadata (BEP-9)   │  │ outbound TCP to N peers, BEP-3 wire  │
│ announce + DHT   │  │ fetch info dict from  │  │ + PEX (BEP-11); STREAMS each SHA-1   │
│ get_peers (BEP-5)│  │ peers → TorrentMeta   │  │ verified piece back as a binary      │
│ → merged peers   │  │                       │  │ frame the moment it completes.       │
└──────────────────┘  └───────────────────────┘  └─────────────────────────────────────┘
```

## How this answers the hard constraints

**"Functions don't retain data."**
They don't need to. The function's only in-memory state (`PieceScheduler`) lives
for the duration of one request and is thrown away. The durable record — which
pieces exist, their bytes, the peer set — is in the browser's IndexedDB
(`public/store.js`). A function invocation is a pure-ish call: `(pieces I want,
peers I know) → (verified pieces, peer health)`.

**"Would the function rediscover peers every invocation? That eats the budget."**
No. Discovery is split into its own endpoint (`/api/announce`) that the client
calls *rarely* — only when its cached peer list drops below a threshold
(`MIN_PEERS`). Announce hits HTTP/UDP trackers **and a one-shot DHT `get_peers`
lookup (BEP-5)** in parallel — the DHT is where most real-world peers come from,
and it works even for trackerless magnets. On top of that, `/api/download`
speaks **PEX (BEP-11)**: already-connected peers gossip their peer lists for
free mid-download, and those flow back to the browser in the stream's summary
frame. The hot path is handed a ready peer list and spends ~all of its ~50 s
budget actually transferring data; the per-peer TCP handshake tax is amortized
across an adaptively sized batch of pieces (8-128 per call).

**"Data has to survive and not get corrupted."**
Every piece is content-addressed by its SHA-1 (from the torrent's `pieces`
field). A piece is verified **twice** — once in the function before it's returned,
and again in the browser (`app.js`) before it is written to IndexedDB. A slow,
buggy, or malicious peer that returns wrong bytes simply fails the hash and the
piece is re-queued. Corruption can never be committed to durable storage. Because
the bitfield + pieces are persisted continuously, a page reload or a killed
function resumes exactly where it left off — pieces already stored are never
re-fetched.

**"Serverless can't own a TCP/IP stack."**
It owns a *partial* one: the Vercel Node runtime can open **outbound** TCP
sockets (`node:net`), which is all a leecher needs. It cannot *listen*, so this is
leech-only (it never seeds and never accepts inbound peers) — an inherent and
acceptable limitation for a download-to-browser tool.

## Layout

| Path | Role |
| --- | --- |
| `lib/bencode.ts` | bencode codec; captures the `info` byte range for infohash |
| `lib/torrent.ts` | `.torrent` → `TorrentMeta` (Node + browser via Web Crypto) |
| `lib/wire.ts` | BEP-3 peer wire protocol framing + `StreamParser` |
| `lib/piece-scheduler.ts` | per-invocation block-level work tracker |
| `lib/peer.ts` | one outbound TCP peer connection (handshake → request → piece) |
| `lib/download-session.ts` | runs many peers under a deadline, SHA-1 verifies pieces |
| `lib/tracker.ts` | HTTP + UDP (BEP-15) announce, merged with DHT results |
| `lib/dht.ts` | one-shot iterative DHT `get_peers` lookup (BEP-5) |
| `lib/frames.ts` | binary streaming frame protocol for `/api/download` |
| `lib/metadata-session.ts` | fetches the info dict from peers via ut_metadata (BEP-9) |
| `api/download.ts` | the bounded downloader function (streams piece frames) |
| `api/announce.ts` | the peer-discovery function (trackers + DHT) |
| `api/metadata.ts` | magnet-link metadata fetch (BEP-9) |
| `public/*` | browser orchestrator: parser, IndexedDB store, worker pool, UI |
| `test/*` | bencode/wire/frames unit tests + a **live mock-seeder integration test** |

## Run the tests

```bash
cd serverless-torrent
npm test          # node --experimental-strip-types --test test/*.test.ts
```

The integration test stands up a real TCP peer that speaks the wire protocol,
runs the actual `runDownload()` against it, and asserts the pieces come back
byte-identical and SHA-1-verified — plus a corrupt-peer case proving bad data is
dropped rather than stored.

## Deploy

```bash
npm i -g vercel
vercel deploy            # static /public + /api functions, zero framework config
```

`vercel.json` sets the function time limits; each function also self-limits below
the platform cap so it always returns partial progress instead of being killed.

## Limitations & honest trade-offs

- **Leech-only.** No seeding and no inbound peers. DHT participation is
  query-only: each announce does a bounded iterative `get_peers` lookup but the
  function never joins the routing table as a node (that needs a persistent UDP
  listener, which serverless can't host).
- **Per-invocation handshake tax.** Sockets can't survive across invocations, so
  every `/api/download` re-handshakes its peers. Batching pieces per call is the
  mitigation; a stickier transport (e.g. one long-lived function via streaming /
  fluid compute, or a tiny stateful relay) would remove it but breaks the
  "pure serverless" model.
- **Egress & cost.** All swarm traffic flows through Vercel's network and counts
  as function egress; this is a design demonstrator, not a cost-optimized CDN.
- **Encryption / NAT.** Plaintext BT only; no MSE/PE, no hole-punching. Many
  peers will be unreachable from a cloud IP — the peer-pruning loop routes
  around them.
- **Legal.** Only use with content you are authorized to distribute.
