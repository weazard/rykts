# serverless-torrent

A **semi-stateless, serverless-native BitTorrent leech** designed for Vercel.

The premise: mainstream BitTorrent clients are stateful long-lived processes that
own a TCP/IP stack and a persistent peer set. Serverless functions are the
opposite — they live <60 s, listen on nothing, and forget everything when they
return. This project splits the client in two so that the half that *needs* to be
stateful runs in the browser, and the half that needs raw outbound TCP runs in a
disposable function.

```
┌─────────────────────────── Browser (durable, long-lived) ───────────────────────────┐
│  • parses the .torrent (piece hashes, layout)                                         │
│  • IndexedDB: verified piece bytes + bitfield + cached peer list + stable peer id     │
│  • orchestrator loop: decides what to fetch next, verifies results, persists          │
└───────────────┬───────────────────────────────────────────────┬──────────────────────┘
                │ POST /api/announce  (rare)                      │ POST /api/download (frequent)
                ▼                                                 ▼
   ┌────────────────────────┐                     ┌──────────────────────────────────────┐
   │ stateless function     │                     │ stateless function                    │
   │ HTTP/UDP tracker       │                     │ outbound TCP to N peers, BEP-3 wire,   │
   │ announce → peer list   │                     │ pull blocks for the requested pieces,  │
   │                        │                     │ return complete pieces (base64) + peer │
   │                        │                     │ health. Self-limits to ~50 s.          │
   └────────────────────────┘                     └──────────────────────────────────────┘
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
(`MIN_PEERS`). The hot path (`/api/download`) is handed a ready peer list and
spends ~all of its ~50 s budget actually transferring data. Within a single
invocation we still pay one TCP handshake per peer, but that cost is amortized
across a whole batch of pieces (`BATCH_PIECES`), not paid per piece.

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
| `lib/tracker.ts` | HTTP + UDP (BEP-15) announce |
| `api/download.ts` | the bounded downloader function |
| `api/announce.ts` | the peer-discovery function |
| `public/*` | browser orchestrator: parser, IndexedDB store, loop, UI |
| `test/*` | bencode/wire unit tests + a **live mock-seeder integration test** |

## Run the tests

```bash
cd serverless-torrent
npm install
npm test          # tsx --test test/*.test.ts
```

Source is TypeScript using the standard NodeNext convention (local imports use
`.js` specifiers that resolve to the `.ts` files), which is what Vercel's function
compiler expects. Tests run through `tsx` so the same specifiers resolve in dev.

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

- **Leech-only.** No seeding, no inbound peers, no DHT (DHT needs a persistent
  UDP node; serverless can't host one). Discovery is tracker-based.
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
- **Tracker reality.** Many popular public torrents (e.g. the WebTorrent "Big
  Buck Bunny") list only `udp://` and `wss://` trackers. `wss://` is a
  WebTorrent/WebRTC tracker whose peers are browser-only and unreachable over
  TCP, so those are skipped; `udp://` works but several well-known ones are dead.
  The announce response reports each tracker's outcome, and the UI logs it, so a
  "0 peers" result is explainable rather than silent. Torrents with a live
  `http(s)://` or `udp://` tracker and conventional TCP seeds work best.
- **Legal.** Only use with content you are authorized to distribute.
