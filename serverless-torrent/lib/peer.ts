// A single outbound TCP connection to one BitTorrent peer, in leech-only mode.
// Serverless functions can open outbound sockets but cannot listen, so we only
// ever download. The connection pulls block requests from a shared
// PieceScheduler and reports completed pieces back through callbacks.
//
// Speed: the request pipeline depth adapts to the peer's observed block rate
// (webtorrent-style bandwidth-delay sizing) instead of a fixed shallow depth —
// fast peers get saturated, slow peers stay shallow so blocks aren't stranded.
//
// Discovery: we speak the BEP 10 extension protocol and understand ut_pex
// (BEP 11), so every healthy connection gossips fresh peers to us for free.

import net from "node:net";
import {
  buildHandshake,
  interested as interestedMsg,
  request as requestMsg,
  extended as extendedMsg,
  handshakeSupportsExtensions,
  parseCompactPeers,
  StreamParser,
  bitfieldHas,
  type ParsedMessage,
} from "./wire.ts";
import { decode as bdecode, encode as bencode, type Bencodable } from "./bencode.ts";
import type { PieceScheduler } from "./piece-scheduler.ts";
import type { PeerAddr } from "./types.ts";

// Pipeline depth bounds. Depth targets rate × RTT-ish window / block size so a
// peer's send window stays full without over-committing blocks to slow peers.
const PIPELINE_MIN = 12;
const PIPELINE_MAX = 64;
const RATE_WINDOW_MS = 500; // sizing horizon: keep ~500ms of blocks in flight
const CONNECT_TIMEOUT_MS = 6000;

// Local extension ids we advertise in our extension handshake (BEP 10). The
// values are arbitrary non-zero bytes; peers echo them back when talking to us.
const OUR_UT_PEX = 1;
const OUR_UT_METADATA = 2;

export interface PeerCallbacks {
  // Called when a piece's blocks are all in; data is unverified.
  onPieceComplete: (index: number, data: Uint8Array) => void;
  // Called once when the peer finishes (cleanly or with error).
  onDone: (health: PeerRunResult) => void;
  // Peers gossiped to us via ut_pex (already deduped per-message, not globally).
  onPeersDiscovered?: (peers: PeerAddr[]) => void;
  // ut_metadata support, used by the magnet metadata fetch path.
  onExtensionHandshake?: (conn: PeerConnection, m: ExtensionHandshakeInfo) => void;
  onMetadataMessage?: (conn: PeerConnection, msg: MetadataMessage) => void;
}

export interface ExtensionHandshakeInfo {
  utMetadata?: number; // peer's id for ut_metadata messages
  utPex?: number; // peer's id for ut_pex messages
  metadataSize?: number; // BEP 9 metadata_size, when the peer knows it
}

export interface MetadataMessage {
  msgType: number; // 0 request, 1 data, 2 reject
  piece: number;
  totalSize?: number;
  data?: Uint8Array; // trailing bytes for msg_type 1
}

export interface PeerRunResult {
  peer: PeerAddr;
  connected: boolean;
  unchoked: boolean;
  piecesServed: number;
  error?: string;
}

export class PeerConnection {
  private socket: net.Socket | null = null;
  private parser = new StreamParser();
  private peerBitfield = new Uint8Array(0);
  private peerHas = new Set<number>(); // covers `have` msgs before/after bitfield
  private amInterested = false;
  private choked = true;
  private outstanding = 0;
  private piecesServed = 0;
  private connected = false;
  private unchokedEver = false;
  private finished = false;

  // Extension protocol state.
  private peerSupportsExtensions = false;
  private peerUtMetadata = 0; // peer's advertised id, 0 = unsupported

  // Adaptive pipeline: track block arrival rate over a sliding window.
  private pipelineDepth = PIPELINE_MIN;
  private blockArrivals: number[] = []; // timestamps of recent block arrivals

  private peer: PeerAddr;
  private infoHash: Uint8Array; // 20 bytes
  private peerId: Uint8Array; // 20 bytes
  private token: number; // unique owner id for scheduler accounting
  private scheduler: PieceScheduler | null;
  private cbs: PeerCallbacks;
  private deadline: number; // epoch ms

  constructor(
    peer: PeerAddr,
    infoHash: Uint8Array,
    peerId: Uint8Array,
    token: number,
    scheduler: PieceScheduler | null,
    cbs: PeerCallbacks,
    deadline: number,
  ) {
    this.peer = peer;
    this.infoHash = infoHash;
    this.peerId = peerId;
    this.token = token;
    this.scheduler = scheduler;
    this.cbs = cbs;
    this.deadline = deadline;
  }

  start(): void {
    const sock = net.connect({ host: this.peer.ip, port: this.peer.port });
    this.socket = sock;
    sock.setTimeout(CONNECT_TIMEOUT_MS);
    sock.setNoDelay(true); // 16 KiB requests shouldn't wait on Nagle

    sock.on("connect", () => {
      this.connected = true;
      sock.setTimeout(0);
      sock.write(buildHandshake(this.infoHash, this.peerId));
    });
    sock.on("timeout", () => this.fail("connect/idle timeout"));
    sock.on("error", (e) => this.fail(e.message));
    sock.on("close", () => this.fail());
    sock.on("data", (chunk: Buffer) => this.onData(new Uint8Array(chunk.buffer, chunk.byteOffset, chunk.length)));
  }

  private onData(chunk: Uint8Array): void {
    if (this.finished) return;
    this.parser.push(chunk);

    if (!this.parser.isHandshakeDone) {
      const hs = this.parser.takeHandshake();
      if (!hs) return; // need more bytes
      if (!eq(hs.infoHash, this.infoHash)) return this.fail("infohash mismatch");
      this.peerSupportsExtensions = handshakeSupportsExtensions(hs);
      if (this.peerSupportsExtensions) this.sendExtensionHandshake();
      // We are interested in everything (we only connected because we want pieces).
      this.sendInterested();
    }

    for (const m of this.parser.messages()) this.handle(m);
    this.pump();
  }

  private handle(m: ParsedMessage): void {
    switch (m.type) {
      case "choke":
        this.choked = true;
        break;
      case "unchoke":
        this.choked = false;
        this.unchokedEver = true;
        break;
      case "bitfield":
        this.peerBitfield = m.bits;
        break;
      case "have":
        this.peerHas.add(m.index);
        break;
      case "piece": {
        this.outstanding = Math.max(0, this.outstanding - 1);
        this.recordBlockArrival();
        if (!this.scheduler) break;
        const full = this.scheduler.onBlock(m.index, m.begin, m.block);
        if (full) {
          this.piecesServed++;
          this.cbs.onPieceComplete(m.index, full);
        }
        break;
      }
      case "extended":
        this.handleExtended(m.extId, m.payload);
        break;
      default:
        break; // keepAlive, request, cancel, port, unknown — ignored (leech-only)
    }
  }

  // --- BEP 10 extension protocol ---

  private sendExtensionHandshake(): void {
    if (!this.socket) return;
    const dict: Record<string, Bencodable> = {
      m: { ut_pex: OUR_UT_PEX, ut_metadata: OUR_UT_METADATA },
    };
    this.socket.write(extendedMsg(0, bencode(dict)));
  }

  private handleExtended(extId: number, payload: Uint8Array): void {
    try {
      if (extId === 0) {
        this.onExtensionHandshake(payload);
      } else if (extId === OUR_UT_PEX) {
        this.onPexMessage(payload);
      } else if (extId === OUR_UT_METADATA) {
        this.onMetadataPayload(payload);
      }
    } catch {
      // Malformed extension payloads are non-fatal; ignore and keep downloading.
    }
  }

  private onExtensionHandshake(payload: Uint8Array): void {
    const { value } = bdecode(payload);
    if (!isDict(value)) return;
    const m = value["m"];
    const info: ExtensionHandshakeInfo = {};
    if (isDict(m)) {
      if (typeof m["ut_metadata"] === "number") {
        this.peerUtMetadata = m["ut_metadata"];
        info.utMetadata = m["ut_metadata"];
      }
      if (typeof m["ut_pex"] === "number") info.utPex = m["ut_pex"];
    }
    if (typeof value["metadata_size"] === "number") info.metadataSize = value["metadata_size"];
    this.cbs.onExtensionHandshake?.(this, info);
  }

  private onPexMessage(payload: Uint8Array): void {
    if (!this.cbs.onPeersDiscovered) return;
    const { value } = bdecode(payload);
    if (!isDict(value)) return;
    const added = value["added"];
    if (!(added instanceof Uint8Array) || added.length === 0) return;
    const peers = parseCompactPeers(added);
    if (peers.length) this.cbs.onPeersDiscovered(peers);
  }

  private onMetadataPayload(payload: Uint8Array): void {
    if (!this.cbs.onMetadataMessage) return;
    // BEP 9: a bencoded dict, then (for msg_type 1) the raw metadata piece bytes.
    const d = bdecode(payload) as { value: Bencodable };
    if (!isDict(d.value)) return;
    const msgType = d.value["msg_type"];
    const piece = d.value["piece"];
    if (typeof msgType !== "number" || typeof piece !== "number") return;
    const msg: MetadataMessage = { msgType, piece };
    if (typeof d.value["total_size"] === "number") msg.totalSize = d.value["total_size"];
    if (msgType === 1) {
      // The dict is a prefix of the payload; the piece data is everything after
      // it. Re-encode the dict to find its byte length (keys are sorted in both).
      const dictLen = bencode(d.value).length;
      msg.data = payload.subarray(dictLen);
    }
    this.cbs.onMetadataMessage(this, msg);
  }

  // Request a metadata piece from this peer (BEP 9), if it supports ut_metadata.
  requestMetadataPiece(piece: number): boolean {
    if (!this.socket || !this.peerUtMetadata) return false;
    const payload = bencode({ msg_type: 0, piece });
    this.socket.write(extendedMsg(this.peerUtMetadata, payload));
    return true;
  }

  get supportsMetadata(): boolean {
    return this.peerUtMetadata !== 0;
  }

  // --- adaptive pipeline ---

  private recordBlockArrival(): void {
    const now = Date.now();
    this.blockArrivals.push(now);
    // Keep only the last ~2s of arrivals; enough signal, bounded memory.
    while (this.blockArrivals.length && now - this.blockArrivals[0] > 2000) {
      this.blockArrivals.shift();
    }
    const windowMs = this.blockArrivals.length > 1 ? now - this.blockArrivals[0] : 0;
    if (windowMs > 100) {
      const blocksPerMs = this.blockArrivals.length / windowMs;
      // Depth ≈ blocks the peer can deliver in RATE_WINDOW_MS, so the pipe
      // stays full while responses are in flight.
      const target = Math.ceil(blocksPerMs * RATE_WINDOW_MS);
      this.pipelineDepth = Math.max(PIPELINE_MIN, Math.min(PIPELINE_MAX, target));
    }
  }

  private sendInterested(): void {
    if (this.amInterested || !this.socket) return;
    this.amInterested = true;
    this.socket.write(interestedMsg());
  }

  // Top up the request pipeline with blocks for pieces this peer has.
  private pump(): void {
    if (this.finished || this.choked || !this.socket || !this.scheduler) return;
    if (Date.now() >= this.deadline) return this.fail();
    while (this.outstanding < this.pipelineDepth) {
      const req = this.scheduler.nextRequest(this.token, (i) => this.has(i));
      if (!req) break;
      this.socket.write(requestMsg(req.index, req.begin, req.length));
      this.outstanding++;
    }
    // Nothing left for anyone and nothing in flight → we're done helping.
    if (this.outstanding === 0 && this.scheduler.pendingPieceCount === 0) this.fail();
  }

  private has(index: number): boolean {
    return this.peerHas.has(index) || bitfieldHas(this.peerBitfield, index);
  }

  private fail(error?: string): void {
    if (this.finished) return;
    this.finished = true;
    this.scheduler?.releaseOwner(this.token);
    if (this.socket) {
      this.socket.removeAllListeners();
      this.socket.destroy();
      this.socket = null;
    }
    this.cbs.onDone({
      peer: this.peer,
      connected: this.connected,
      unchoked: this.unchokedEver,
      piecesServed: this.piecesServed,
      error,
    });
  }

  stop(): void {
    this.fail();
  }
}

function eq(a: Uint8Array, b: Uint8Array): boolean {
  if (a.length !== b.length) return false;
  for (let i = 0; i < a.length; i++) if (a[i] !== b[i]) return false;
  return true;
}

function isDict(v: Bencodable | undefined): v is Record<string, Bencodable> {
  return !!v && typeof v === "object" && !Array.isArray(v) && !(v instanceof Uint8Array);
}
