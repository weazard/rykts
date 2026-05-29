// A single outbound TCP connection to one BitTorrent peer, in leech-only mode.
// Serverless functions can open outbound sockets but cannot listen, so we only
// ever download. The connection pulls block requests from a shared
// PieceScheduler and reports completed pieces back through callbacks.

import net from "node:net";
import {
  buildHandshake,
  interested as interestedMsg,
  request as requestMsg,
  StreamParser,
  bitfieldHas,
  type ParsedMessage,
} from "./wire.ts";
import type { PieceScheduler } from "./piece-scheduler.ts";
import type { PeerAddr } from "./types.ts";

const PIPELINE_DEPTH = 8; // outstanding block requests per peer
const CONNECT_TIMEOUT_MS = 6000;

export interface PeerCallbacks {
  // Called when a piece's blocks are all in; data is unverified.
  onPieceComplete: (index: number, data: Uint8Array) => void;
  // Called once when the peer finishes (cleanly or with error).
  onDone: (health: PeerRunResult) => void;
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

  private peer: PeerAddr;
  private infoHash: Uint8Array; // 20 bytes
  private peerId: Uint8Array; // 20 bytes
  private token: number; // unique owner id for scheduler accounting
  private scheduler: PieceScheduler;
  private cbs: PeerCallbacks;
  private deadline: number; // epoch ms

  constructor(
    peer: PeerAddr,
    infoHash: Uint8Array,
    peerId: Uint8Array,
    token: number,
    scheduler: PieceScheduler,
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
        const full = this.scheduler.onBlock(m.index, m.begin, m.block);
        if (full) {
          this.piecesServed++;
          this.cbs.onPieceComplete(m.index, full);
        }
        break;
      }
      default:
        break; // keepAlive, request, cancel, port, unknown — ignored (leech-only)
    }
  }

  private sendInterested(): void {
    if (this.amInterested || !this.socket) return;
    this.amInterested = true;
    this.socket.write(interestedMsg());
  }

  // Top up the request pipeline with blocks for pieces this peer has.
  private pump(): void {
    if (this.finished || this.choked || !this.socket) return;
    if (Date.now() >= this.deadline) return this.fail();
    while (this.outstanding < PIPELINE_DEPTH) {
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
    this.scheduler.releaseOwner(this.token);
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
