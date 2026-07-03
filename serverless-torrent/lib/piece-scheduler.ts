// Tracks block-level progress for the set of pieces a single /api/download
// invocation is trying to fetch. Peers pull block requests from it and push
// received blocks back; it assembles whole pieces for the caller to SHA-1 verify.
//
// This object is the in-memory "state" of one invocation — it is created at the
// start of the request and discarded when the function returns. Durable state
// lives in the browser, not here.

import { BLOCK_SIZE } from "./wire.ts";

type BlockState = 0 | 1 | 2; // 0 need, 1 requested, 2 done

interface PieceProgress {
  index: number;
  hash: string; // expected hex SHA-1
  length: number;
  blockStates: BlockState[];
  requestedAt: number[]; // ms timestamp when a block was requested
  requestOwner: (number | null)[]; // peer token that owns an outstanding request
  buffer: Uint8Array;
  received: number; // count of done blocks
}

export interface BlockRequest {
  index: number;
  begin: number;
  length: number;
}

const REQUEST_TIMEOUT_MS = 8000;
// When this few pieces remain in the work set, peers may duplicate in-flight
// block requests instead of idling (endgame mode).
const ENDGAME_PIECE_THRESHOLD = 4;

export class PieceScheduler {
  private pieces = new Map<number, PieceProgress>();
  private order: number[] = [];

  constructor(
    wanted: { index: number; hash: string }[],
    pieceLength: number,
    totalLength: number,
  ) {
    const pieceCount = Math.ceil(totalLength / pieceLength);
    const lastIndex = pieceCount - 1;
    const lastLen = totalLength - lastIndex * pieceLength;
    for (const w of wanted) {
      const length = w.index === lastIndex ? lastLen : pieceLength;
      const numBlocks = Math.ceil(length / BLOCK_SIZE);
      this.pieces.set(w.index, {
        index: w.index,
        hash: w.hash,
        length,
        blockStates: new Array(numBlocks).fill(0),
        requestedAt: new Array(numBlocks).fill(0),
        requestOwner: new Array(numBlocks).fill(null),
        buffer: new Uint8Array(length),
        received: 0,
      });
      this.order.push(w.index);
    }
  }

  get pendingPieceCount(): number {
    return this.pieces.size;
  }

  // Pick the next block to request for a peer that has piece `index` (hasFn).
  // Returns null when this peer has nothing useful to request right now.
  //
  // Endgame: when few pieces remain and this peer has no fresh blocks to claim,
  // allow it to *duplicate* a block already in flight with another peer (BEP 3
  // endgame mode). Without this, the last blocks of an invocation stall behind
  // one slow peer for up to REQUEST_TIMEOUT_MS. Duplicates are cheap (16 KiB)
  // and onBlock() ignores the loser.
  nextRequest(owner: number, hasFn: (index: number) => boolean): BlockRequest | null {
    const fresh = this.pickBlock(owner, hasFn, false);
    if (fresh) return fresh;
    if (this.pendingPieceCount <= ENDGAME_PIECE_THRESHOLD) {
      return this.pickBlock(owner, hasFn, true);
    }
    return null;
  }

  private pickBlock(
    owner: number,
    hasFn: (index: number) => boolean,
    allowDuplicates: boolean,
  ): BlockRequest | null {
    const now = Date.now();
    for (const idx of this.order) {
      const p = this.pieces.get(idx);
      if (!p) continue;
      if (!hasFn(idx)) continue;
      for (let b = 0; b < p.blockStates.length; b++) {
        const st = p.blockStates[b];
        if (st === 2) continue;
        if (st === 1) {
          const stale = now - p.requestedAt[b] >= REQUEST_TIMEOUT_MS;
          const dup = allowDuplicates && p.requestOwner[b] !== owner;
          if (!stale && !dup) continue;
        }
        // claim it (fresh need, a timed-out request, or an endgame duplicate)
        p.blockStates[b] = 1;
        p.requestedAt[b] = now;
        p.requestOwner[b] = owner;
        const begin = b * BLOCK_SIZE;
        const length = Math.min(BLOCK_SIZE, p.length - begin);
        return { index: idx, begin, length };
      }
    }
    return null;
  }

  // Store a received block. Returns the assembled piece bytes if this completed
  // it (caller must SHA-1 verify before trusting), otherwise null.
  onBlock(index: number, begin: number, block: Uint8Array): Uint8Array | null {
    const p = this.pieces.get(index);
    if (!p) return null;
    const b = Math.floor(begin / BLOCK_SIZE);
    if (b < 0 || b >= p.blockStates.length) return null;
    if (p.blockStates[b] === 2) return null; // duplicate
    p.buffer.set(block.subarray(0, Math.min(block.length, p.length - begin)), begin);
    p.blockStates[b] = 2;
    p.requestOwner[b] = null;
    p.received++;
    if (p.received === p.blockStates.length) return p.buffer;
    return null;
  }

  // Piece passed verification: remove it from the work set.
  complete(index: number): void {
    this.pieces.delete(index);
    this.order = this.order.filter((i) => i !== index);
  }

  // Piece failed verification: reset all its blocks so it can be re-fetched.
  reset(index: number): void {
    const p = this.pieces.get(index);
    if (!p) return;
    p.blockStates.fill(0);
    p.requestedAt.fill(0);
    p.requestOwner.fill(null);
    p.buffer = new Uint8Array(p.length);
    p.received = 0;
  }

  // Free outstanding requests owned by a peer that disconnected/errored.
  releaseOwner(owner: number): void {
    for (const p of this.pieces.values()) {
      for (let b = 0; b < p.blockStates.length; b++) {
        if (p.blockStates[b] === 1 && p.requestOwner[b] === owner) {
          p.blockStates[b] = 0;
          p.requestOwner[b] = null;
        }
      }
    }
  }
}
